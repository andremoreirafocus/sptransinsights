Este projeto faz:
- lê cada um dos arquivos em um prefixo de uma pasta raw no minio

Configurações:
GTFS_URL = "http://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS"
LOGIN = <insira seu login>
PASSWORD = <insira sua senha>
LOCAL_DOWNLOADS_FOLDER = "gtfs_files"
RAW_BUCKET_NAME = "raw"
APP_FOLDER = "gtfs"
TRUSTED_BUCKET_NAME = "trusted"

Para instalar os requisitos:
pip install -r requirements.txt

Para executar: 
python ./main.py
