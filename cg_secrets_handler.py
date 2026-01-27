import os
from inputs.cg_log import setup_logger
# Azure SDK imports
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.keyvault.certificates import CertificateClient
from cryptography.hazmat.primitives.serialization import pkcs12, Encoding, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend
import base64
import ssl
import tempfile
import requests
from requests.adapters import HTTPAdapter
from azure.keyvault.secrets import SecretClient

logger = setup_logger(__name__)


def _get_config(key: str) -> str | None:
    # Prefer real environment variables first, then fall back to Airflow Variables if available.
    val = os.getenv(key)
    if val:
        return val
    else:
        logger.debug(f"Environment variable {key} not set, checking Airflow Variables")
        try:
            from airflow.models import Variable
            return Variable.get(key, default_var=None)
        except ImportError:
            logger.debug("Airflow not available, cannot check Airflow Variables")
            return None


def _get_credential_and_vault_url():
    """
    Reads tenant_id, client_id, and client_secret from environment variables,
    and retrieves the Key Vault name from params.yml.
    Returns (credential, vault_url).
    """
    try:
        # # 1. Path to params.yml (adjust according to your structure)
        # yaml_path = Path(__file__).parent / "inputs" / "params.yml"
        # with open(yaml_path, "r") as file:
        #     yml_file = yaml.safe_load(file)
        # azure_key_vault = yml_file["AZURE"]["key_vault"]

        # 2. Read environment variables
        tenant_id = _get_config("TENANT_ID")
        client_id = _get_config("CLIENT_ID")
        client_secret = _get_config("SECRET_KEYVAULT")
        keyvault_name = _get_config("KEYVAULT_NAME")

        # logger.debug(f"Using tenant_id: {tenant_id}")
        # logger.debug(f"Using client_id: {client_id}")
        # logger.debug(f"Using client_secret: {client_secret}")
        # logger.debug(f"Using keyvault_name: {keyvault_name}")
        # 3. Create credentials
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        # 4. Build the Key Vault URL
        vault_url = f"https://{keyvault_name}.vault.azure.net"
        # logger.debug(f"Using vault URL: {vault_url}")
        # logger.debug(f"Using vault_url: {vault_url}")
        return credential, vault_url
    except Exception as e:
        logger.error(f"Error getting key vault credential, error-> {e}")
        return None, None


def get_secret_value(secret_name: str) -> str:
    """
    Retrieves the value of a secret (string) stored in Azure Key Vault.

    :param secret_name: The name (alias) of the secret in Key Vault.
    :return: The secret's value as a string.
    """
    try:

        credential, vault_url = _get_credential_and_vault_url()
        # logger.debug(f"Using vault URL: {vault_url}")
        # logger.debug(f"Using secret name: {secret_name}")
        # logger.debug(f"Using credential: {credential}")
        secret_client = SecretClient(vault_url=vault_url, credential=credential)

        retrieved_secret = secret_client.get_secret(secret_name)
        # logger.debug(f"Using retrieved_secret: {retrieved_secret}")
        return retrieved_secret.value

    except Exception as e:
        logger.error(f"Error getting secret value, error-> {e}")
        return None


def get_certificate_pem(certificate_name: str) -> bytes:
    """
    Retrieves the public certificate (DER format) from Key Vault
    and returns it as PEM bytes (optional).
    If you only want the raw DER, you can return `cert_bundle.cer` instead.

    :param certificate_name: The name (alias) of the certificate in Key Vault.
    :return: The certificate in PEM format (bytes), or None if .cer is not found.
    """
    try:
        credential, vault_url = _get_credential_and_vault_url()
        cert_client = CertificateClient(vault_url=vault_url, credential=credential)

        # Call get_certificate
        cert_bundle = cert_client.get_certificate(certificate_name)

        if cert_bundle.cer is None:
            # Could happen if the certificate has no public part
            return None

        # cert_bundle.cer is DER bytes
        public_der = cert_bundle.cer

        # Convert DER -> PEM, if desired
        import base64
        pem_data = (
                b"-----BEGIN CERTIFICATE-----\n" +
                base64.encodebytes(public_der) +
                b"-----END CERTIFICATE-----\n"
        )

        return pem_data
    except Exception as e:
        logger.error(f"Error getting certificate pem, error-> {e}")
        return None


def _build_ssl_context(private_key, certificate, additional_certs, ca_pem_text: str | None = None) -> ssl.SSLContext:
    pem_key = private_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())
    pem_cert = certificate.public_bytes(Encoding.PEM)
    chain = pem_cert + b"".join(c.public_bytes(Encoding.PEM) for c in (additional_certs or []))

    key_file = tempfile.NamedTemporaryFile(delete=False, suffix=".key")
    cert_file = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
    key_file.write(pem_key);
    key_file.flush()
    cert_file.write(chain);
    cert_file.flush()

    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.load_cert_chain(certfile=cert_file.name, keyfile=key_file.name)

    # <- IMPORTANT: trust OMIE root in this context (so adapter traffic validates against it)
    if ca_pem_text:
        ctx.load_verify_locations(cadata=ca_pem_text)

    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


def get_ca_pem_text_from_kv(secret_name: str) -> str | None:
    val = get_secret_value(secret_name)
    if not val:
        return None
    if "BEGIN CERTIFICATE" in val:
        return val
    # if it’s base64-encoded DER in the secret, decode and convert
    try:
        raw = base64.b64decode(val, validate=True)
        try:
            from OpenSSL import crypto
            x = crypto.load_certificate(crypto.FILETYPE_ASN1, raw)
            return crypto.dump_certificate(crypto.FILETYPE_PEM, x).decode("utf-8")
        except Exception:
            text = raw.decode("utf-8", errors="ignore")
            return text if "BEGIN CERTIFICATE" in text else None
    except Exception:
        return None


def create_session_with_pfx(cert_name: str, password_secret_name: str, ca_secret_name: str | None = None) -> requests.Session:
    credential, vault_url = _get_credential_and_vault_url()
    secret_client = SecretClient(vault_url=vault_url, credential=credential)

    secret_bundle = secret_client.get_secret(cert_name)
    pfx_bytes = base64.b64decode(secret_bundle.value)

    pw_bundle = secret_client.get_secret(password_secret_name)
    pw = pw_bundle.value.strip()
    password_bytes = pw.encode("utf-8") if pw else None

    last_error = None
    for candidate in (password_bytes, None):
        try:
            private_key, certificate, additional_certs = pkcs12.load_key_and_certificates(
                pfx_bytes, candidate, backend=default_backend()
            )
            if private_key and certificate:
                break
        except Exception as e:
            last_error = e
    else:
        raise ValueError(f"Failed to load PFX from KV: {last_error}")

    ca_pem_text = get_ca_pem_text_from_kv(ca_secret_name) if ca_secret_name else None
    ctx = _build_ssl_context(private_key, certificate, additional_certs, ca_pem_text=ca_pem_text)

    class _Adapter(HTTPAdapter):
        def init_poolmanager(self, *args, **kwargs):
            kwargs["ssl_context"] = ctx
            return super().init_poolmanager(*args, **kwargs)

        def proxy_manager_for(self, *args, **kwargs):
            kwargs["ssl_context"] = ctx
            return super().proxy_manager_for(*args, **kwargs)

    session = requests.Session()
    session.mount("https://", _Adapter())
    return session