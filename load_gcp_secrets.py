import os
import sys
from google.cloud import secretmanager

def load_env_from_secret(project_id, secret_id, env_file_path):
    """
    Fetches the .env file from Google Secret Manager, saves it locally, and loads it into the environment.
    """
    client = secretmanager.SecretManagerServiceClient()
    
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    
    # Decode secret value (content of .env file)
    env_content = response.payload.data.decode("UTF-8")

    # Save the .env file permanently
    env_file_path = os.path.expanduser(env_file_path)  # Expand ~ to home directory
    with open(env_file_path, "w") as env_file:
        env_file.write(env_content)

    print(f"File saved at {env_file_path}")
    print("Environment variables loaded.")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python script.py <PROJECT_ID> <ENV_VAR> <ENV_PATH>")
        sys.exit(1)

    # The path where the .env file will be saved
    PROJECT_ID = sys.argv[1]
    ENV_VAR = sys.argv[2]
    ENV_PATH = sys.argv[3]
    
    load_env_from_secret(PROJECT_ID, ENV_VAR, ENV_PATH)
