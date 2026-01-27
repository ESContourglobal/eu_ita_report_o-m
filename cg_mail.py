import base64
import mimetypes
import os
import requests
from msal import ConfidentialClientApplication
from cg_secrets_handler import get_secret_value
from inputs.cg_log import logger


def authenticate_email_confidential():
    client_id = get_secret_value("CLIENT-ID")
    client_secret = get_secret_value("CLIENT-SECRET")
    tenant_id = get_secret_value("TENANT-ID")

    authority = f"https://login.microsoftonline.com/{tenant_id}"
    scope = ["https://graph.microsoft.com/.default"]

    app = ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority
    )
    result = app.acquire_token_for_client(scopes=scope)

    if "access_token" not in result:
        logger.info(f"MSAL error: {result.get('error')}: {result.get('error_description')}")
        return None
    return result["access_token"]


def _guess_mime(path, default="application/octet-stream"):
    mime, _ = mimetypes.guess_type(path)
    return mime or default


def _file_to_b64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def _make_inline_attachment(path, cid, content_type=None, name=None):
    if not content_type:
        content_type = _guess_mime(path)
    if not name:
        name = os.path.basename(path)
    return {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": name,
        "contentType": content_type,
        "contentBytes": _file_to_b64(path),
        "isInline": True,
        "contentId": cid,  # must exactly match <img src="cid:...">
    }


def _wrap_html(body_html: str) -> str:
    # Some Outlook clients behave better with full HTML doc structure.
    return f"<html><body>{body_html}</body></html>"

def _make_file_attachment(path: str, display_name: str | None = None, content_type: str | None = None) -> dict:
    """
    Build a simple file attachment for Microsoft Graph sendMail.
    Suitable for small files (Graph's JSON upload limit applies).
    """
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    if display_name is None:
        display_name = os.path.basename(path)

    if content_type is None:
        guessed, _ = mimetypes.guess_type(path)
        content_type = guessed or "application/octet-stream"

    with open(path, "rb") as f:
        content_bytes = base64.b64encode(f.read()).decode("utf-8")

    return {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": display_name,
        "contentType": content_type,
        "contentBytes": content_bytes,
    }



def send_email(
    subject: str,
    body: str,
    recipients: list[str],
    bcc: list[str] | None = None,
    images: dict[str, str] | None = None,   # {"Line_Graph": line_path, "Combined_Table": combo_path}
    attachments: list[str] | None = None,
):
    """
    Sends an email with optional inline images provided as a mapping {cid: file_path}.
    """
    token = authenticate_email_confidential()
    if not token:
        logger.info("Auth failed")
        return False

    from_address = get_secret_value("EMAILCOMMERCIALUPN")

    # Build inline attachments from dict {cid: path}
    graph_attachments = []
    if images:
        for cid, path in images.items():
            if not path or not os.path.exists(path):
                logger.info(f"Inline image missing or not found: cid={cid}, path={path}")
                continue
            graph_attachments.append(_make_inline_attachment(path, cid))

    # Build simple file attachments
    file_attachments = []
    if attachments:
        try:
            # Normalize to iterable of (display_name, path)
            items: list[tuple[str, str]] = []
            if isinstance(attachments, dict):
                items = [(name, pth) for name, pth in attachments.items()]
            elif isinstance(attachments, list):
                if attachments and isinstance(attachments[0], tuple):
                    items = [(name, pth) for name, pth in attachments]  # type: ignore[arg-type]
                else:
                    items = [(None, pth) for pth in attachments]  # type: ignore[list-item]
            else:
                logger.info("Unsupported attachments type; must be list[str] | dict[str,str] | list[tuple[str,str]]")
                items = []

            for disp, pth in items:
                if not pth or not os.path.exists(pth):
                    logger.info(f"Attachment missing or not found: name={disp}, path={pth}")
                    continue
                try:
                    file_attachments.append(_make_file_attachment(pth, display_name=disp))
                except Exception as e:
                    logger.info(f"Failed to build file attachment for {pth}: {e}")
        except Exception as e:
            logger.info(f"Error processing attachments: {e}")

    # Ensure body references images and is proper HTML
    # IMPORTANT: Graph expects "HTML" or "Text" (casing matters in some clients)
    # If body already contains <html>, keep it; otherwise wrap.
    content_type = "HTML"
    body_html = body or ""
    # If you passed plain text like " ", you’ll get red X; build tags if empty but images exist
    if (not body_html.strip()) and graph_attachments:
        # Auto-build simple body with the cids provided
        cids = [att["contentId"] for att in graph_attachments]
        img_tags = "".join(f'<div><img src="cid:{c}"></div>' for c in cids)
        body_html = img_tags

    if "<html" not in body_html.lower():
        body_html = _wrap_html(body_html)

    message = {
        "subject": subject,
        "body": {"contentType": content_type, "content": body_html},
        "bccRecipients": [{"emailAddress": {"address": r}} for r in recipients],
    }
    merged_attachments = []
    if graph_attachments:
        merged_attachments.extend(graph_attachments)
    if file_attachments:
        merged_attachments.extend(file_attachments)
    if merged_attachments:
        message["attachments"] = merged_attachments

    email_payload = {"message": message, "saveToSentItems": True}

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{from_address}/sendMail",
        headers=headers,
        json=email_payload,
        timeout=30,
    )

    if resp.status_code == 202:
        logger.info("Email sent successfully")
        return True
    else:
        logger.info(f"Failed to send email. Status Code: {resp.status_code}")
        logger.info(resp.text)
        return False