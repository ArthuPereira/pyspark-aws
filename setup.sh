#!/usr/bin/env bash
set -euo pipefail

# Carregar variáveis do .env se existir
if [ -f .env ]; then
    echo "[INFO] Carregando variáveis do arquivo .env..."
    set -a
    source .env
    set +a
fi

export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# Verificar autenticação na AWS
echo "[INFO] Verificando credenciais AWS na região ${AWS_DEFAULT_REGION}..."
if ! aws sts get-caller-identity; then
    echo "[ERRO] Falha ao autenticar na AWS. Verifique suas credenciais no .env ou se a sessão do Learner Lab expirou."
    exit 1
fi

# Definir ou recuperar nome do Bucket
BUCKET_FILE=".bucket_name"
if [ -z "${BUCKET:-}" ]; then
    if [ -f "$BUCKET_FILE" ]; then
        export BUCKET=$(cat "$BUCKET_FILE")
        echo "[INFO] Reutilizando bucket salvo em ${BUCKET_FILE}: ${BUCKET}"
    else
        USER_CLEAN=$(whoami | tr -cd '[:alnum:]-' | tr '[:upper:]' '[:lower:]')
        export BUCKET="lab-spark-${USER_CLEAN}-${RANDOM}"
        echo "$BUCKET" > "$BUCKET_FILE"
        echo "[INFO] Novo nome de bucket gerado e salvo em ${BUCKET_FILE}: ${BUCKET}"
    fi
else
    echo "$BUCKET" > "$BUCKET_FILE"
    echo "[INFO] Usando variável BUCKET definida no ambiente: ${BUCKET}"
fi

# Criar o Bucket S3 (se ainda não existir)
if aws s3 ls "s3://${BUCKET}" >/dev/null 2>&1; then
    echo "[INFO] O bucket s3://${BUCKET} já existe."
else
    echo "[INFO] Criando bucket s3://${BUCKET}..."
    aws s3 mb "s3://${BUCKET}" --region "$AWS_DEFAULT_REGION"
fi

# Definir anos a baixar (padrão: 2024; aceita anos como parâmetros: ./setup.sh 2023 2024)
if [ $# -eq 0 ]; then
    YEARS=(2024)
else
    YEARS=("$@")
fi

MONTHS=(01 02 03 04 05 06 07 08 09 10 11 12)

DATA_DIR="data"
mkdir -p "${DATA_DIR}/trips" "${DATA_DIR}/zones"

URL="https://d37ci6vzurychx.cloudfront.net"

echo "[INFO] Baixando arquivos Parquet para o(s) ano(s): ${YEARS[*]} (meses 01 a 12)..."
for y in "${YEARS[@]}"; do
    for m in "${MONTHS[@]}"; do
        FILE="${DATA_DIR}/trips/yellow_${y}-${m}.parquet"
        REMOTE_URL="${URL}/trip-data/yellow_tripdata_${y}-${m}.parquet"
        if [ -f "$FILE" ]; then
            echo "  -> [SKIP] ${FILE} já existe localmente."
        else
            echo "  -> Baixando yellow_${y}-${m}.parquet..."
            if ! curl -fL --progress-bar -o "$FILE" "$REMOTE_URL"; then
                echo "  -> [AVISO] Arquivo de ${y}-${m} não disponível na origem (pulando)."
                rm -f "$FILE"
            fi
        fi
    done
done

ZONE_FILE="${DATA_DIR}/zones/taxi_zone_lookup.csv"
if [ -f "$ZONE_FILE" ]; then
    echo "  -> [SKIP] ${ZONE_FILE} já existe localmente."
else
    echo "[INFO] Baixando tabela de zonas (taxi_zone_lookup.csv)..."
    curl -fL -s -o "$ZONE_FILE" "${URL}/misc/taxi_zone_lookup.csv"
fi

# Enviar dados brutos (camada raw) e script PySpark para o S3
echo "[INFO] Enviando arquivos Parquet para s3://${BUCKET}/raw/trips/..."
aws s3 cp "${DATA_DIR}/trips/" "s3://${BUCKET}/raw/trips/" --recursive --exclude "*" --include "*.parquet"

echo "[INFO] Enviando arquivo de zonas para s3://${BUCKET}/raw/zones/..."
aws s3 cp "$ZONE_FILE" "s3://${BUCKET}/raw/zones/"

if [ -f "taxi_job.py" ]; then
    echo "[INFO] Enviando script taxi_job.py para s3://${BUCKET}/scripts/..."
    aws s3 cp taxi_job.py "s3://${BUCKET}/scripts/taxi_job.py"
else
    echo "[AVISO] Arquivo taxi_job.py não encontrado no diretório atual."
fi

# Resumo final dos objetos no S3
echo "[SUCESSO] Setup concluído! Conteúdo atual do bucket s3://${BUCKET}:"
echo ""
# aws s3 ls "s3://${BUCKET}/" --recursive --human-readable
echo ""
echo "Para usar a variável BUCKET no seu terminal atual, rode:"
echo "  export BUCKET=$(cat "$BUCKET_FILE")"
