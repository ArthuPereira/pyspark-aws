#!/usr/bin/env bash
set -euo pipefail

# Carregar variáveis de ambiente (.env se existir)
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# Recuperar BUCKET (.bucket_name ou variável de ambiente)
if [ -z "${BUCKET:-}" ] && [ -f .bucket_name ]; then
    export BUCKET=$(cat .bucket_name)
fi

ACTION="${1:-create}"

create_cluster() {
    if [ -z "${BUCKET:-}" ]; then
        echo "[ERRO] Variável BUCKET não definida. Rode ./setup.sh primeiro ou exporte BUCKET=..."
        exit 1
    fi

    # Seleciona automaticamente uma Subnet em us-east-1a/b/c/d (evitando us-east-1e onde m5.xlarge falha)
    SUBNET_ID="${SUBNET_ID:-$(aws ec2 describe-subnets \
        --filters "Name=availability-zone,Values=us-east-1a,us-east-1b,us-east-1c,us-east-1d" \
        --query "Subnets[0].SubnetId" --output text)}"

    # Detecta automaticamente as Roles disponíveis na conta (EMR_DefaultRole vs LabRole)
    if aws iam get-role --role-name EMR_DefaultRole >/dev/null 2>&1; then
        ROLE_ARGS=(--use-default-roles --ec2-attributes "KeyName=vockey,SubnetId=${SUBNET_ID}")
        ROLE_DESC="--use-default-roles (EMR_DefaultRole)"
    else
        if aws iam get-instance-profile --instance-profile-name LabInstanceProfile >/dev/null 2>&1; then
            IP_NAME="LabInstanceProfile"
        else
            IP_NAME="LabRole"
        fi
        ROLE_ARGS=(--service-role LabRole --ec2-attributes "KeyName=vockey,InstanceProfile=${IP_NAME},SubnetId=${SUBNET_ID}")
        ROLE_DESC="LabRole / ${IP_NAME}"
    fi

    # Contorna diferença de versão da AWS CLI (v1 antiga do Academy vs v2)
    AUTO_TERM_ARGS=()
    if aws emr create-cluster help 2>&1 | grep -q -- "--auto-termination-policy"; then
        AUTO_TERM_ARGS=(--auto-termination-policy IdleTimeout=1800)
        CLI_AUTO_TERM="suportado nativamente na CLI"
    else
        CLI_AUTO_TERM="CLI antiga detectada (flag omitida no create-cluster)"
    fi

    INSTANCE_TYPE="${INSTANCE_TYPE:-m5.xlarge}"
    RELEASE_LABEL="${RELEASE_LABEL:-emr-7.2.0}"

    echo "[INFO] Criando cluster EMR..."
    echo "  -> Bucket:           s3://${BUCKET}"
    echo "  -> Release / Tipo:   ${RELEASE_LABEL} / ${INSTANCE_TYPE} (3 nós)"
    echo "  -> SubnetId:         ${SUBNET_ID}"
    echo "  -> Roles IAM:        ${ROLE_DESC}"
    echo "  -> Auto-termination: ${CLI_AUTO_TERM}"

    CLUSTER_ID=$(aws emr create-cluster \
        --name "lab-spark-$(whoami)" \
        --release-label "$RELEASE_LABEL" \
        --applications Name=Spark \
        --instance-type "$INSTANCE_TYPE" \
        --instance-count 3 \
        "${ROLE_ARGS[@]}" \
        ${AUTO_TERM_ARGS[@]+"${AUTO_TERM_ARGS[@]}"} \
        --log-uri "s3://${BUCKET}/logs/" \
        --steps "Type=Spark,Name=taxi-job,ActionOnFailure=CONTINUE,Args=[s3://${BUCKET}/scripts/taxi_job.py,${BUCKET}]" \
        --query ClusterId --output text)

    echo "$CLUSTER_ID" > cluster_id.txt
    echo "export CLUSTER=${CLUSTER_ID}" >> ~/.bashrc 2>/dev/null || true

    # Se a AWS CLI não suportou a flag no create-cluster, tenta aplicar via subcomando ou boto3
    if [ ${#AUTO_TERM_ARGS[@]} -eq 0 ]; then
        if aws emr put-auto-termination-policy --cluster-id "$CLUSTER_ID" --auto-termination-policy IdleTimeout=1800 >/dev/null 2>&1; then
            echo "[INFO] Auto-termination (1800s) aplicado via put-auto-termination-policy."
        elif python3 -c "import boto3; boto3.client('emr', region_name='${AWS_DEFAULT_REGION}').put_auto_termination_policy(ClusterId='${CLUSTER_ID}', AutoTerminationPolicy={'IdleTimeout': 1800})" >/dev/null 2>&1; then
            echo "[INFO] Auto-termination (1800s) aplicado via Python boto3."
        else
            echo "[AVISO] Versão antiga da AWS CLI/boto3 não suporta IdleTimeout via script. Lembre-se de rodar './emr.sh terminate' ao finalizar."
        fi
    fi

    echo ""
    echo "[SUCESSO] Cluster criado: ${CLUSTER_ID} (salvo em cluster_id.txt)"
}

get_cluster_id() {
    if [ -z "${CLUSTER:-}" ] && [ -f cluster_id.txt ]; then
        export CLUSTER=$(cat cluster_id.txt)
    fi
    if [ -z "${CLUSTER:-}" ]; then
        echo "[ERRO] Nenhum cluster encontrado em cluster_id.txt ou na variável CLUSTER."
        exit 1
    fi
}

case "$ACTION" in
    create)
        create_cluster
        ;;

    8-particoes)
        get_cluster_id
        if [ -z "${BUCKET:-}" ]; then
            echo "[ERRO] Variável BUCKET não encontrada."
            exit 1
        fi
        echo "[INFO] Adicionando Step 2 (taxi-job-8p com 8 partições de shuffle) no cluster ${CLUSTER}..."
        aws emr add-steps \
            --cluster-id "$CLUSTER" \
            --steps "Type=Spark,Name=taxi-job-8p,ActionOnFailure=CONTINUE,Args=[--conf,spark.sql.shuffle.partitions=8,s3://${BUCKET}/scripts/taxi_job.py,${BUCKET}]"
        ;;

    terminate)
        get_cluster_id
        echo "[INFO] Encerrando cluster ${CLUSTER}..."
        aws emr terminate-clusters --cluster-ids "$CLUSTER"
        aws emr describe-cluster --cluster-id "$CLUSTER" --query Cluster.Status.State --output text
        ;;

    *)
        echo "Uso: ./emr.sh [create | 8-particoes | terminate]"
        exit 1
        ;;
esac
