from dotenv import dotenv_values
from gtfs import download_gtfs
from monitorar_onibus import monitorar_onibus


def main():
    config = dotenv_values(".env")
    download_gtfs(login=config.get("LOGIN"), password=config.get("PASSWORD"))
    monitorar_onibus(
        token=config.get("TOKEN"),
        base_url=config.get("BASE_URL"),
        intervalo=int(config.get("INTERVALO")),
    )


if __name__ == "__main__":
    main()
