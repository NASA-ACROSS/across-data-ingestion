import os


def create_env_file():
    env_file_path = ".env"

    env_vars = {
        "ACROSS_INGESTION_SERVICE_ACCOUNT_KEY": "local-ingestion-service-account-key",
        "ACROSS_SERVER_URL": "http://localhost:8000/api/v1/",
        "ACROSS_DEBUG": True,
    }

    if not os.path.exists(env_file_path):
        with open(env_file_path, "w") as env_file:
            for key, value in env_vars.items():
                env_file.write(f"{key}={value}\n")
        print(f"Created {env_file_path} with default values.")
    else:
        print(f"{env_file_path} already exists.")


if __name__ == "__main__":
    create_env_file()
