##URL : https://developer.hashicorp.com/vault/install

Vault lets you securely store secrets (like DB passwords, API keys) and access them using API calls or CLI — from any cloud, any app, anywhere.

✅ Step-by-Step: Host a Vault Server on Your System
🔧 1. Install Vault on your machine
For Windows/macOS/Linux:

Go to https://developer.hashicorp.com/vault/downloads

Download & extract Vault binary.

Add Vault to system PATH.

Verify:

bash
Copy
Edit
vault --version
🚀 2. Start a local Vault Dev Server
For testing:

bash
Copy
Edit
vault server -dev
You’ll get output like:

yaml
Copy
Edit
Root Token: s.1234567890abcdef
This token is needed to access the secrets.

🔐 3. Set environment variables for Vault CLI
bash
Copy
Edit
export VAULT_ADDR='http://127.0.0.1:8200'
export VAULT_TOKEN='s.1234567890abcdef'
📦 4. Store a secret
bash
Copy
Edit
vault kv put secret/mydb user='admin' password='mypassword'
🔍 5. Read the secret from Python (anywhere)
Install:

bash
Copy
Edit
pip install hvac
Python code:

python
Copy
Edit
import hvac

client = hvac.Client(
    url='http://<YOUR_PUBLIC_IP>:8200',  # change this to your public IP
    token='s.1234567890abcdef'
)

secret = client.secrets.kv.read_secret_version(path='secret/mydb')
print(secret['data']['data'])  # {'user': 'admin', 'password': 'mypassword'}
🌐 Make Vault Accessible from Anywhere
To use Vault from AWS/Databricks:

Allow firewall rule to expose port 8200 (cautiously).

Run behind NGINX reverse proxy with TLS.

Use HTTPS only and enable authentication (AppRole, tokens, etc.).

🔄 Alternative: Use AWS Secrets Manager (If you want to avoid hosting)
If hosting a Vault is too complex:

You can use AWS Secrets Manager and access secrets in:

Databricks

Python scripts via boto3

Spark jobs via IAM role or spark.conf

🛡️ Security Tips
Never hardcode secrets in scripts.

Use IAM or token-based access only.

Store vault token securely or use dynamic tokens.

For long-term use, configure AppRole or OIDC login.
