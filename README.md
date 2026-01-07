Este projeto faz download de informações da SPTRANS:
- arquivos GTFS do portal do desenvolvedor
- posição dos ônibus de SP usando a API da SPTRANS

Credenciais necessárias se encontram no arquivo .env do projeto usando as seguintes variáveis:
API_BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
TOKEN =  <insira o seu token>
INTERVALO = 120  # 2 minutos em segundos
GTFS_URL = "http://www.sptrans.com.br/umbraco/Surface/PerfilDesenvolvedor/BaixarGTFS"
LOGIN = <insira seu login>
PASSWORD = <insira sua senha>

Para instalar os requisitos:
pip install -r requirements.txt

Para executar: 
python ./main.py
