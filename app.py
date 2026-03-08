# app.py
# ==========================================================
# Interfaz de Control Económico — PDFs + Previsión
# - Login con desplegable (usuarios desde BD)
# - Roles: Administrador (owner), Empleado/a (employee), Comercial (commercial)
# - Comerciales: solo ven SUS ventas firmadas
# - Admin: ve todas + resumen, puede registrar venta "en nombre de" un comercial
# - Catálogo modelos: crear + editar + renombrar código (corregir errores)
# - Usuarios: ver PIN, crear, cambiar PIN, eliminar (con confirmación)
# - Logo arriba derecha: logo.png (misma carpeta que app.py)
# ==========================================================

import json
import os
import re
from datetime import datetime, date
from pathlib import Path
from io import BytesIO
import unicodedata
import xml.etree.ElementTree as ET

import pandas as pd
import requests
import streamlit as st
import pdfplumber
from sqlalchemy import create_engine, text

# =======================
# CONFIG
# =======================
st.set_page_config(page_title="Control Económico — PDFs + Previsión", layout="wide")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "control_economico.db"
DB_URL = f"sqlite:///{DB_PATH.as_posix()}"
engine = create_engine(DB_URL, future=True)

DATA_DIR = Path("data")
PDF_DIR = DATA_DIR / "pdfs"
PDF_DIR.mkdir(parents=True, exist_ok=True)
ATTACHMENTS_DIR = DATA_DIR / "purchase_invoice_attachments"
ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)

LOGO_PATH = Path("logo.png")  # coloca logo.png junto a app.py

CURRENT_YEAR = date.today().year
HIST_YEARS = [CURRENT_YEAR - i for i in range(1, 5)]
MONTHS = [f"{m:02d}" for m in range(1, 13)]

DOC_TYPES = [
    "BALANCE_ANUAL",
    "PYG_ANUAL",
    "EXTRACTO_CIERRE",
    "COSTES_LABORALES_ANUAL",
    "PRESTAMOS_DEUDAS_CIERRE",
]

# Roles internos (BD)
ROLE_OWNER = "owner"         # Administrador
ROLE_EMPLOYEE = "employee"   # Empleado/a
ROLE_COMMERCIAL = "commercial"

ROLE_LABELS = {
    ROLE_OWNER: "Administrador",
    ROLE_EMPLOYEE: "Empleado/a",
    ROLE_COMMERCIAL: "Comercial",
}

VALID_ROLES = {ROLE_OWNER, ROLE_EMPLOYEE, ROLE_COMMERCIAL}


ENTITY_TYPE_LABELS = {
    "customer": "Cliente",
    "supplier": "Proveedor",
    "sales_invoice": "Factura emitida",
    "purchase_invoice": "Factura recibida",
}

ACTION_LABELS = {
    "create": "Crear",
    "update": "Actualizar",
    "delete": "Eliminar",
}

SYNC_STATUS_LABELS = {
    "pending": "Pendiente",
    "processing": "Procesando",
    "done": "Completado",
    "error": "Error",
}

INVOICE_STATUS_LABELS = {
    "draft": "Borrador",
    "issued": "Emitida",
    "cancelled": "Cancelada",
}


def header_with_logo(title: str):
    left, right = st.columns([6, 1])
    with left:
        st.title(title)
    with right:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), use_container_width=True)


header_with_logo("Interfaz de Control Económico — PDFs + Previsión")


# =======================
# DB INIT + MIGRATIONS
# =======================
def _table_exists(conn, name: str) -> bool:
    r = conn.execute(
        text("SELECT name FROM sqlite_master WHERE type='table' AND name=:n"),
        {"n": name},
    ).fetchone()
    return r is not None


def _columns(conn, table: str) -> list[str]:
    cols = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    # pragma: cid, name, type, notnull, dflt_value, pk
    return [c[1] for c in cols]


def migrate_users_drop_check_if_needed():
    """
    Si existía una tabla antigua con CHECK(role IN ('owner','employee')), la recreamos sin CHECK.
    """
    with engine.begin() as conn:
        if not _table_exists(conn, "users"):
            return
        ddl = conn.execute(
            text("SELECT sql FROM sqlite_master WHERE type='table' AND name='users'")
        ).fetchone()
        if not ddl or not ddl[0]:
            return
        sql = ddl[0]
        if "CHECK(role IN" in sql:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users_new(
                    username TEXT PRIMARY KEY,
                    pin TEXT NOT NULL,
                    role TEXT NOT NULL
                );
            """))
            conn.execute(text("""
                INSERT INTO users_new(username, pin, role)
                SELECT username, pin, role FROM users;
            """))
            conn.execute(text("DROP TABLE users;"))
            conn.execute(text("ALTER TABLE users_new RENAME TO users;"))


def init_or_migrate_db():
    migrate_users_drop_check_if_needed()

    with engine.begin() as conn:
        # USERS
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS users(
            username TEXT PRIMARY KEY,
            pin TEXT NOT NULL,
            role TEXT NOT NULL
        );
        """))

        # YEARS
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS fiscal_years(
            year INTEGER PRIMARY KEY,
            is_closed INTEGER NOT NULL DEFAULT 0,
            closed_at TEXT,
            note TEXT
        );
        """))

        # DOCUMENTS
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS documents(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uploaded_at TEXT NOT NULL,
            uploaded_by TEXT NOT NULL,
            doc_type TEXT NOT NULL,
            year INTEGER NOT NULL,
            scope TEXT NOT NULL, -- HISTORICAL/CURRENT
            period TEXT NOT NULL, -- ANUAL o YYYY-MM
            bank TEXT NOT NULL DEFAULT '',
            filepath TEXT NOT NULL,
            status TEXT NOT NULL,
            parse_log TEXT
        );
        """))

        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_documents_unique
        ON documents(scope, doc_type, year, period, bank);
        """))

        # EXTRACTED ROWS
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS extracted_rows(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            document_id INTEGER NOT NULL,
            row_type TEXT NOT NULL,
            row_key TEXT,
            row_date TEXT,
            description TEXT,
            amount REAL,
            currency TEXT DEFAULT 'EUR',
            extra_json TEXT,
            FOREIGN KEY(document_id) REFERENCES documents(id)
        );
        """))

        # MODELS
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS models(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            fuel TEXT NOT NULL,
            list_price REAL NOT NULL,
            active INTEGER NOT NULL DEFAULT 1
        );
        """))

                # EXPENSE FORECAST (budget)
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS expense_forecast(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            period TEXT NOT NULL,       -- YYYY-MM
            category TEXT NOT NULL,
            amount REAL NOT NULL,       -- positivo (gasto)
            notes TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            updated_at TEXT,
            updated_by TEXT
        );
        """))

        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_expense_forecast_unique
        ON expense_forecast(year, period, category);
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_expense_forecast_yp
        ON expense_forecast(year, period);
        """))

# SIGNED SALES
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS signed_sales(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            year INTEGER NOT NULL,
            period TEXT NOT NULL,
            customer TEXT NOT NULL,
            model_id INTEGER,              -- OJO: lo migramos si ya existía sin esta columna
            units INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            notes TEXT,
            status TEXT NOT NULL DEFAULT 'signed',
            FOREIGN KEY(model_id) REFERENCES models(id)
        );
        """))

        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_signed_sales_ym
        ON signed_sales(year, period);
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_signed_sales_by
        ON signed_sales(created_by, year, period);
        """))

        # --- MIGRACIONES LIGERAS ---
        # Si signed_sales existía sin model_id:
        if _table_exists(conn, "signed_sales"):
            cols = _columns(conn, "signed_sales")
            if "model_id" not in cols:
                conn.execute(text("ALTER TABLE signed_sales ADD COLUMN model_id INTEGER"))

        # CUSTOMERS
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS customers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            tax_id TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            address TEXT NOT NULL DEFAULT '',
            city TEXT NOT NULL DEFAULT '',
            postal_code TEXT NOT NULL DEFAULT '',
            province TEXT NOT NULL DEFAULT '',
            country TEXT NOT NULL DEFAULT 'ES',
            notes TEXT NOT NULL DEFAULT '',
            active INTEGER NOT NULL DEFAULT 1,
            external_id_mygestion TEXT,
            sync_status TEXT NOT NULL DEFAULT 'pending',
            sync_error TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            updated_at TEXT,
            updated_by TEXT
        );
        """))
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_customers_tax_id
        ON customers(tax_id)
        WHERE tax_id <> '';
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_customers_name
        ON customers(name);
        """))

        # SUPPLIERS
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS suppliers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            tax_id TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            address TEXT NOT NULL DEFAULT '',
            city TEXT NOT NULL DEFAULT '',
            postal_code TEXT NOT NULL DEFAULT '',
            province TEXT NOT NULL DEFAULT '',
            country TEXT NOT NULL DEFAULT 'ES',
            notes TEXT NOT NULL DEFAULT '',
            active INTEGER NOT NULL DEFAULT 1,
            external_id_mygestion TEXT,
            sync_status TEXT NOT NULL DEFAULT 'pending',
            sync_error TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            updated_at TEXT,
            updated_by TEXT
        );
        """))
        conn.execute(text("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_suppliers_tax_id
        ON suppliers(tax_id)
        WHERE tax_id <> '';
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_suppliers_name
        ON suppliers(name);
        """))

        # SALES INVOICES
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS sales_invoices(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_number TEXT NOT NULL UNIQUE,
            invoice_date TEXT NOT NULL,
            customer_id INTEGER NOT NULL,
            base_amount REAL NOT NULL,
            tax_rate REAL NOT NULL DEFAULT 21.0,
            tax_amount REAL NOT NULL,
            total_amount REAL NOT NULL,
            notes TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'draft',
            external_id_mygestion TEXT,
            sync_status TEXT NOT NULL DEFAULT 'pending',
            sync_error TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            updated_at TEXT,
            updated_by TEXT,
            FOREIGN KEY(customer_id) REFERENCES customers(id)
        );
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_sales_invoices_date
        ON sales_invoices(invoice_date, status);
        """))

        # FACTURAS RECIBIDAS
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS purchase_invoices(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_invoice_number TEXT NOT NULL,
            invoice_date TEXT NOT NULL,
            supplier_id INTEGER NOT NULL,
            base_amount REAL NOT NULL,
            tax_rate REAL NOT NULL DEFAULT 21.0,
            tax_amount REAL NOT NULL,
            total_amount REAL NOT NULL,
            deductible INTEGER NOT NULL DEFAULT 1,
            notes TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'draft',
            attachment_path TEXT,
            attachment_name TEXT,
            attachment_mime TEXT,
            attachment_uploaded_at TEXT,
            attachment_uploaded_by TEXT,
            external_id_mygestion TEXT,
            sync_status TEXT NOT NULL DEFAULT 'pending',
            sync_error TEXT,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL,
            updated_at TEXT,
            updated_by TEXT,
            FOREIGN KEY(supplier_id) REFERENCES suppliers(id)
        );
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_purchase_invoices_date
        ON purchase_invoices(invoice_date, status);
        """))

        purchase_cols = _columns(conn, "purchase_invoices")
        if "attachment_path" not in purchase_cols:
            conn.execute(text("ALTER TABLE purchase_invoices ADD COLUMN attachment_path TEXT"))
        if "attachment_name" not in purchase_cols:
            conn.execute(text("ALTER TABLE purchase_invoices ADD COLUMN attachment_name TEXT"))
        if "attachment_mime" not in purchase_cols:
            conn.execute(text("ALTER TABLE purchase_invoices ADD COLUMN attachment_mime TEXT"))
        if "attachment_uploaded_at" not in purchase_cols:
            conn.execute(text("ALTER TABLE purchase_invoices ADD COLUMN attachment_uploaded_at TEXT"))
        if "attachment_uploaded_by" not in purchase_cols:
            conn.execute(text("ALTER TABLE purchase_invoices ADD COLUMN attachment_uploaded_by TEXT"))

        # SYNC QUEUE
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS sync_queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            payload_json TEXT,
            created_at TEXT NOT NULL,
            processed_at TEXT
        );
        """))
        conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_sync_queue_status
        ON sync_queue(status, entity_type, created_at);
        """))


def ensure_defaults():
    """
    Inserta defaults SIEMPRE con INSERT OR IGNORE (no duplica).
    Así evitamos el caso: ya había usuarios pero no comerciales.
    """
    with engine.begin() as conn:
        # usuarios por defecto
        defaults_users = [
            ("owner", "1234", ROLE_OWNER),
            ("empleado1", "0000", ROLE_EMPLOYEE),
            ("empleado2", "0000", ROLE_EMPLOYEE),
            ("comercial1", "1111", ROLE_COMMERCIAL),
            ("comercial2", "2222", ROLE_COMMERCIAL),
        ]
        for u, p, r in defaults_users:
            conn.execute(text("""
                INSERT OR IGNORE INTO users(username, pin, role)
                VALUES(:u,:p,:r)
            """), {"u": u, "p": p, "r": r})

        # años fiscales
        for y in HIST_YEARS + [CURRENT_YEAR]:
            conn.execute(
                text("INSERT OR IGNORE INTO fiscal_years(year, is_closed) VALUES(:y,0)"),
                {"y": int(y)},
            )

        # modelos por defecto
        defaults_models = [
            ("303", "Estufa pellet modelo 303", "pellet", 1990.00),
            ("115", "Estufa pellet modelo 115", "pellet", 1490.00),
            ("111", "Estufa pellet modelo 111", "pellet", 1390.00),
            ("190", "Estufa leña modelo 190", "leña", 1790.00),
            ("200", "Estufa leña modelo 200", "leña", 1890.00),
        ]
        for code, name, fuel, price in defaults_models:
            conn.execute(text("""
                INSERT OR IGNORE INTO models(code, name, fuel, list_price, active)
                VALUES(:c,:n,:f,:p,1)
            """), {"c": code, "n": name, "f": fuel, "p": float(price)})


init_or_migrate_db()
ensure_defaults()


# =======================
# MYGESTION HELPERS
# =======================
def _secret_or_env(name: str, default: str = "") -> str:
    try:
        value = st.secrets.get(name, default)
        if value is None:
            value = default
        value = str(value).strip()
        if value:
            return value
    except Exception:
        pass
    return str(os.getenv(name, default) or "").strip()


def mygestion_settings() -> dict:
    base_url = _secret_or_env("MYGESTION_BASE_URL", "").rstrip("/")
    username = _secret_or_env("MYGESTION_USERNAME", "")
    password = _secret_or_env("MYGESTION_PASSWORD", "")
    endpoints = {
        "customer": _secret_or_env("MYGESTION_ENDPOINT_CUSTOMERS", "ApiClientes"),
        "supplier": _secret_or_env("MYGESTION_ENDPOINT_SUPPLIERS", "ApiProveedores"),
        "sales_invoice": _secret_or_env("MYGESTION_ENDPOINT_SALES_INVOICES", "ApiFacturasVenta"),
        "purchase_invoice": _secret_or_env("MYGESTION_ENDPOINT_PURCHASE_INVOICES", "ApiFacturasCompra"),
    }
    return {
        "base_url": base_url,
        "username": username,
        "password": password,
        "endpoints": endpoints,
        "enabled": bool(base_url and username and password),
    }


def _mygestion_session(cfg: dict) -> requests.Session:
    s = requests.Session()
    s.auth = (cfg["username"], cfg["password"])
    s.headers.update({"Accept": "application/xml"})
    return s


def _mygestion_endpoint_url(cfg: dict, entity_type: str, external_id: str | None = None) -> str:
    endpoint = cfg["endpoints"][entity_type].strip("/")
    base = f"{cfg['base_url'].rstrip('/')}/{endpoint}"
    if external_id:
        return f"{base}/{external_id}"
    return base


def _xml_find_first(root: ET.Element, names: list[str]) -> ET.Element | None:
    wanted = {n.lower() for n in names}
    for el in root.iter():
        tag = el.tag.split('}')[-1].lower()
        if tag in wanted:
            return el
    return None


def _xml_set_first(root: ET.Element, names: list[str], value) -> bool:
    el = _xml_find_first(root, names)
    if el is None:
        return False
    el.text = "" if value is None else str(value)
    return True


def _mygestion_fetch_blank_schema(cfg: dict, entity_type: str) -> ET.Element:
    s = _mygestion_session(cfg)
    url = _mygestion_endpoint_url(cfg, entity_type)
    r = s.get(url, params={"schema": "blank"}, timeout=30)
    r.raise_for_status()
    return ET.fromstring(r.content)


def _partner_to_schema_xml(root: ET.Element, payload: dict) -> bytes:
    mapping = {
        "name": ["nombre", "razonsocial", "razon_social", "name"],
        "tax_id": ["cif", "nif", "dni", "vatnumber", "vat_number", "tax_id"],
        "email": ["email", "mail"],
        "phone": ["telefono", "movil", "phone"],
        "address": ["direccion", "address"],
        "city": ["poblacion", "ciudad", "city"],
        "postal_code": ["codpostal", "cp", "postal_code", "zipcode"],
        "province": ["provincia", "province"],
        "country": ["pais", "country"],
        "notes": ["observaciones", "notas", "notes"],
        "active": ["activo", "active"],
    }
    for key, candidates in mapping.items():
        _xml_set_first(root, candidates, payload.get(key, ""))
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _sales_invoice_to_schema_xml(root: ET.Element, invoice: dict, customer_external_id: str | None) -> bytes:
    mapping = {
        "invoice_number": ["numero", "numerofactura", "factura", "invoice_number"],
        "invoice_date": ["fecha", "fechafactura", "invoice_date"],
        "notes": ["observaciones", "notas", "notes"],
        "base_amount": ["baseimponible", "base", "importe_base", "base_amount"],
        "tax_amount": ["cuotaiva", "iva", "tax_amount"],
        "tax_rate": ["porcentajeiva", "tipoiva", "tax_rate"],
        "total_amount": ["total", "importe_total", "total_amount"],
        "status": ["estado", "status"],
    }
    for key, candidates in mapping.items():
        _xml_set_first(root, candidates, invoice.get(key, ""))
    if customer_external_id:
        _xml_set_first(root, ["idcliente", "cliente", "customer_id", "id_customer"], customer_external_id)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _purchase_invoice_to_schema_xml(root: ET.Element, invoice: dict, supplier_external_id: str | None) -> bytes:
    mapping = {
        "supplier_invoice_number": ["numero", "numerofactura", "factura", "supplier_invoice_number", "invoice_number"],
        "invoice_date": ["fecha", "fechafactura", "invoice_date"],
        "notes": ["observaciones", "notas", "notes"],
        "base_amount": ["baseimponible", "base", "importe_base", "base_amount"],
        "tax_amount": ["cuotaiva", "iva", "tax_amount"],
        "tax_rate": ["porcentajeiva", "tipoiva", "tax_rate"],
        "total_amount": ["total", "importe_total", "total_amount"],
        "status": ["estado", "status"],
        "deductible": ["deducible", "is_deductible", "deductible"],
    }
    for key, candidates in mapping.items():
        value = invoice.get(key, "")
        if key == "deductible":
            value = 1 if bool(value) else 0
        _xml_set_first(root, candidates, value)
    if supplier_external_id:
        _xml_set_first(root, ["idproveedor", "proveedor", "supplier_id", "id_supplier"], supplier_external_id)
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def mygestion_test_connection() -> tuple[bool, str]:
    cfg = mygestion_settings()
    if not cfg["enabled"]:
        return False, "Configura MYGESTION_BASE_URL, MYGESTION_USERNAME y MYGESTION_PASSWORD."
    try:
        _mygestion_fetch_blank_schema(cfg, "customer")
        return True, "Conexión OK. Se ha podido leer el esquema del recurso de clientes."
    except Exception as e:
        return False, f"Error de conexión: {e}"


def _mygestion_upsert_partner(entity_type: str, payload: dict, external_id: str | None = None) -> str:
    cfg = mygestion_settings()
    root = _mygestion_fetch_blank_schema(cfg, entity_type)
    xml_body = _partner_to_schema_xml(root, payload)
    s = _mygestion_session(cfg)
    if external_id:
        r = s.put(_mygestion_endpoint_url(cfg, entity_type, external_id), data=xml_body, headers={"Content-Type": "application/xml"}, timeout=30)
        r.raise_for_status()
        return str(external_id)
    r = s.post(_mygestion_endpoint_url(cfg, entity_type), data=xml_body, headers={"Content-Type": "application/xml"}, timeout=30)
    r.raise_for_status()
    try:
        rr = ET.fromstring(r.content)
        found = _xml_find_first(rr, ["id", "codigo", "idcliente", "idproveedor"])
        if found is not None and (found.text or "").strip():
            return found.text.strip()
    except Exception:
        pass
    loc = r.headers.get("Location", "").rstrip("/")
    return loc.split("/")[-1] if loc else ""


def _mygestion_upsert_sales_invoice(invoice: dict, external_id: str | None = None) -> str:
    cfg = mygestion_settings()
    customer = get_customer(int(invoice["customer_id"]))
    if not customer:
        raise ValueError("La factura no tiene un cliente válido.")
    customer_external_id = str(customer.get("external_id_mygestion") or "").strip()
    if not customer_external_id:
        raise ValueError("El cliente de la factura todavía no está sincronizado con myGestión.")
    root = _mygestion_fetch_blank_schema(cfg, "sales_invoice")
    xml_body = _sales_invoice_to_schema_xml(root, invoice, customer_external_id)
    s = _mygestion_session(cfg)
    if external_id:
        r = s.put(_mygestion_endpoint_url(cfg, "sales_invoice", external_id), data=xml_body, headers={"Content-Type": "application/xml"}, timeout=30)
        r.raise_for_status()
        return str(external_id)
    r = s.post(_mygestion_endpoint_url(cfg, "sales_invoice"), data=xml_body, headers={"Content-Type": "application/xml"}, timeout=30)
    r.raise_for_status()
    try:
        rr = ET.fromstring(r.content)
        found = _xml_find_first(rr, ["id", "codigo", "idfactura", "idfacturaventa"])
        if found is not None and (found.text or "").strip():
            return found.text.strip()
    except Exception:
        pass
    loc = r.headers.get("Location", "").rstrip("/")
    return loc.split("/")[-1] if loc else ""


def _mygestion_upsert_purchase_invoice(invoice: dict, external_id: str | None = None) -> str:
    cfg = mygestion_settings()
    supplier = get_supplier(int(invoice["supplier_id"]))
    if not supplier:
        raise ValueError("La factura recibida no tiene un proveedor válido.")
    supplier_external_id = str(supplier.get("external_id_mygestion") or "").strip()
    if not supplier_external_id:
        raise ValueError("El proveedor de la factura todavía no está sincronizado con myGestión.")
    root = _mygestion_fetch_blank_schema(cfg, "purchase_invoice")
    xml_body = _purchase_invoice_to_schema_xml(root, invoice, supplier_external_id)
    s = _mygestion_session(cfg)
    if external_id:
        r = s.put(_mygestion_endpoint_url(cfg, "purchase_invoice", external_id), data=xml_body, headers={"Content-Type": "application/xml"}, timeout=30)
        r.raise_for_status()
        return str(external_id)
    r = s.post(_mygestion_endpoint_url(cfg, "purchase_invoice"), data=xml_body, headers={"Content-Type": "application/xml"}, timeout=30)
    r.raise_for_status()
    try:
        rr = ET.fromstring(r.content)
        found = _xml_find_first(rr, ["id", "codigo", "idfactura", "idfacturacompra"])
        if found is not None and (found.text or "").strip():
            return found.text.strip()
    except Exception:
        pass
    loc = r.headers.get("Location", "").rstrip("/")
    return loc.split("/")[-1] if loc else ""


def _update_entity_sync(entity_type: str, entity_id: int, status: str, external_id: str | None = None, error: str | None = None):
    table = {"customer": "customers", "supplier": "suppliers", "sales_invoice": "sales_invoices", "purchase_invoice": "purchase_invoices"}[entity_type]
    sets = ["sync_status=:st", "sync_error=:err"]
    params = {"id": int(entity_id), "st": status, "err": (error or None)}
    if external_id is not None:
        sets.append("external_id_mygestion=:eid")
        params["eid"] = external_id
    with engine.begin() as conn:
        conn.execute(text(f"UPDATE {table} SET {', '.join(sets)} WHERE id=:id"), params)


def process_sync_item(queue_id: int) -> tuple[bool, str]:
    cfg = mygestion_settings()
    with engine.begin() as conn:
        row = conn.execute(text("SELECT * FROM sync_queue WHERE id=:id"), {"id": int(queue_id)}).mappings().first()
    if not row:
        return False, "No existe el registro en cola."
    item = dict(row)
    if not cfg["enabled"]:
        msg = "Falta la configuración de myGestión. Revisa MYGESTION_BASE_URL, MYGESTION_USERNAME y MYGESTION_PASSWORD."
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE sync_queue SET status='error', attempts=attempts+1, last_error=:err, processed_at=:pa WHERE id=:id"),
                {"id": int(queue_id), "err": msg, "pa": datetime.now().isoformat(timespec="seconds")},
            )
        try:
            _update_entity_sync(item["entity_type"], int(item["entity_id"]), "error", error=msg)
        except Exception:
            pass
        return False, msg
    try:
        update_sync_status(queue_id, "processing", "")
        entity_type = item["entity_type"]
        entity_id = int(item["entity_id"])
        action = item["action"]

        if entity_type == "customer":
            entity = get_customer(entity_id)
            if not entity:
                raise ValueError("Cliente no encontrado.")
            external_id = _mygestion_upsert_partner("customer", entity, entity.get("external_id_mygestion") if action == "update" else None)
            _update_entity_sync("customer", entity_id, "synced", external_id=external_id, error=None)
        elif entity_type == "supplier":
            entity = get_supplier(entity_id)
            if not entity:
                raise ValueError("Proveedor no encontrado.")
            external_id = _mygestion_upsert_partner("supplier", entity, entity.get("external_id_mygestion") if action == "update" else None)
            _update_entity_sync("supplier", entity_id, "synced", external_id=external_id, error=None)
        elif entity_type == "sales_invoice":
            entity = get_sales_invoice(entity_id)
            if not entity:
                raise ValueError("Factura emitida no encontrada.")
            external_id = _mygestion_upsert_sales_invoice(entity, entity.get("external_id_mygestion") if action == "update" else None)
            _update_entity_sync("sales_invoice", entity_id, "synced", external_id=external_id, error=None)
        elif entity_type == "purchase_invoice":
            entity = get_purchase_invoice(entity_id)
            if not entity:
                raise ValueError("Factura recibida no encontrada.")
            external_id = _mygestion_upsert_purchase_invoice(entity, entity.get("external_id_mygestion") if action == "update" else None)
            _update_entity_sync("purchase_invoice", entity_id, "synced", external_id=external_id, error=None)
        else:
            raise ValueError(f"Tipo de entidad no soportado: {entity_type}")

        with engine.begin() as conn:
            conn.execute(text("UPDATE sync_queue SET status='done', attempts=attempts+1, last_error=NULL, processed_at=:pa WHERE id=:id"), {"id": int(queue_id), "pa": datetime.now().isoformat(timespec="seconds")})
        return True, "Sincronizado correctamente."
    except Exception as e:
        msg = str(e)
        with engine.begin() as conn:
            conn.execute(text("UPDATE sync_queue SET status='error', attempts=attempts+1, last_error=:err, processed_at=:pa WHERE id=:id"), {"id": int(queue_id), "err": msg, "pa": datetime.now().isoformat(timespec="seconds")})
        try:
            _update_entity_sync(item["entity_type"], int(item["entity_id"]), "error", error=msg)
        except Exception:
            pass
        return False, msg


def process_sync_batch(limit: int = 20) -> tuple[int, int, list[str]]:
    qdf = sync_queue_df(status="pending", limit=limit)
    ok = 0
    err = 0
    messages = []
    for queue_id in qdf["id"].astype(int).tolist():
        done, msg = process_sync_item(queue_id)
        if done:
            ok += 1
        else:
            err += 1
            messages.append(f"#{queue_id}: {msg}")
    return ok, err, messages


# =======================
# HELPERS DB
# =======================
def read_df(query: str, params=None) -> pd.DataFrame:
    params = params or {}
    with engine.begin() as conn:
        rows = conn.execute(text(query), params).mappings().all()
    return pd.DataFrame(rows)


def list_users_for_login() -> list[str]:
    df = read_df("SELECT username FROM users ORDER BY username")
    return df["username"].tolist() if not df.empty else []


def list_commercial_users() -> list[str]:
    df = read_df("SELECT username FROM users WHERE role=:r ORDER BY username", {"r": ROLE_COMMERCIAL})
    return df["username"].tolist() if not df.empty else []


def year_is_closed(y: int) -> bool:
    with engine.begin() as conn:
        r = conn.execute(
            text("SELECT is_closed FROM fiscal_years WHERE year=:y"),
            {"y": int(y)},
        ).mappings().first()
    return bool(r and r["is_closed"] == 1)


def doc_exists(scope: str, doc_type: str, year: int, period: str, bank: str) -> bool:
    bank = bank or ""
    with engine.begin() as conn:
        r = conn.execute(
            text("""
            SELECT id FROM documents
            WHERE scope=:s AND doc_type=:dt AND year=:y AND period=:p AND bank=:b
            LIMIT 1
            """),
            {"s": scope, "dt": doc_type, "y": int(year), "p": period, "b": bank},
        ).mappings().first()
    return r is not None


def insert_document(uploaded_by, doc_type, year, scope, period, bank, filepath, status, parse_log=""):
    bank = bank or ""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO documents(uploaded_at, uploaded_by, doc_type, year, scope, period, bank, filepath, status, parse_log)
            VALUES(:at,:by,:dt,:y,:sc,:p,:b,:fp,:st,:log)
        """), {
            "at": datetime.now().isoformat(timespec="seconds"),
            "by": uploaded_by,
            "dt": doc_type,
            "y": int(year),
            "sc": scope,
            "p": period,
            "b": bank,
            "fp": str(filepath),
            "st": status,
            "log": parse_log,
        })
        doc_id = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()["id"]
    return int(doc_id)


def clear_extracted_rows(document_id: int):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM extracted_rows WHERE document_id=:d"), {"d": int(document_id)})


def insert_extracted_rows(document_id: int, rows: list[dict]):
    if not rows:
        return
    with engine.begin() as conn:
        for r in rows:
            conn.execute(text("""
                INSERT INTO extracted_rows(document_id, row_type, row_key, row_date, description, amount, currency, extra_json)
                VALUES(:did,:rt,:rk,:rd,:desc,:amt,:cur,:ex)
            """), {
                "did": int(document_id),
                "rt": r.get("row_type"),
                "rk": r.get("row_key"),
                "rd": r.get("row_date"),
                "desc": r.get("description"),
                "amt": r.get("amount"),
                "cur": r.get("currency", "EUR"),
                "ex": json.dumps(r.get("extra", {}), ensure_ascii=False),
            })


# =======================
# AUTH
# =======================
def login(username: str, pin: str):
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT username, pin, role FROM users WHERE username=:u"),
            {"u": username.strip()},
        ).mappings().first()
    if not row or row["role"] not in VALID_ROLES:
        return False, None
    return ((pin or "") == (row["pin"] or "")), row["role"] if ((pin or "") == (row["pin"] or "")) else (False, None)


def require_role(allowed_roles):
    if not st.session_state.get("auth", False):
        st.stop()
    if st.session_state["role"] not in allowed_roles:
        st.error("No tienes permisos para ver esta sección.")
        st.stop()


# =======================
# PDF SPOTTING
# =======================
def pdf_to_text(filepath: str) -> str:
    parts = []
    with pdfplumber.open(filepath) as pdf:
        for page in pdf.pages:
            parts.append(page.extract_text() or "")
    return "\n".join(parts).strip()


def pdf_file_to_text(file_obj) -> str:
    parts = []
    try:
        raw = file_obj.getvalue() if hasattr(file_obj, "getvalue") else file_obj.read()
        bio = BytesIO(raw)
        with pdfplumber.open(bio) as pdf:
            for page in pdf.pages:
                parts.append(page.extract_text() or "")
    except Exception:
        return ""
    return "\n".join(parts).strip()


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9]+", " ", s).strip().lower()
    return re.sub(r"\s+", " ", s)


def _clean_tax_id(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", str(s or "")).upper()


def _extract_first(patterns: list[str], text_value: str, flags=re.IGNORECASE):
    for pat in patterns:
        m = re.search(pat, text_value, flags)
        if m:
            for g in m.groups():
                if g:
                    return g.strip()
    return ""


def parse_purchase_invoice_pdf(file_obj) -> dict:
    text_value = pdf_file_to_text(file_obj)
    result = {
        "ok": bool(text_value),
        "text": text_value,
        "supplier_invoice_number": "",
        "invoice_date": date.today().isoformat(),
        "base_amount": 0.0,
        "tax_rate": 21.0,
        "tax_amount": 0.0,
        "total_amount": 0.0,
        "notes": "",
        "supplier_id": None,
        "supplier_match_label": "",
        "warnings": [],
    }
    if not text_value:
        result["warnings"].append("No se pudo extraer texto del PDF. Si es escaneado, habrá que rellenarlo manualmente.")
        return result

    invoice_number = _extract_first([
        r"(?:factura|n[ºo°]|num(?:ero)?\.?\s*factura)\s*[:#-]?\s*([A-Z0-9\-/]{3,})",
        r"(?:serie|ref(?:erencia)?)\s*[:#-]?\s*([A-Z0-9\-/]{3,})",
    ], text_value)
    result["supplier_invoice_number"] = invoice_number
    if not invoice_number:
        result["warnings"].append("No se detectó con seguridad el número de factura.")

    date_raw = _extract_first([
        r"(?:fecha\s*factura|fecha\s*emision|fecha)\s*[:#-]?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"(\d{4}-\d{2}-\d{2})",
        r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
    ], text_value)
    if date_raw:
        try:
            result["invoice_date"] = pd.to_datetime(date_raw, dayfirst=True).date().isoformat()
        except Exception:
            result["warnings"].append(f"No se pudo normalizar la fecha detectada: {date_raw}")

    def labeled_amount(patterns):
        for pat in patterns:
            m = re.search(pat, text_value, re.IGNORECASE)
            if m:
                amt = parse_amount_es(m.group(1))
                if amt is not None:
                    return round(float(abs(amt)), 2)
        return None

    base_amount = labeled_amount([
        r"base\s+imponible\s*[:=]?\s*([\d\.,]+)",
        r"subtotal\s*[:=]?\s*([\d\.,]+)",
        r"base\s*[:=]?\s*([\d\.,]+)",
    ])
    tax_amount = labeled_amount([
        r"cuota\s+iva\s*[:=]?\s*([\d\.,]+)",
        r"iva\s*(?:\d{1,2}[\.,]?\d{0,2}%?)?\s*[:=]?\s*([\d\.,]+)",
    ])
    total_amount = labeled_amount([
        r"total\s+factura\s*[:=]?\s*([\d\.,]+)",
        r"importe\s+total\s*[:=]?\s*([\d\.,]+)",
        r"total\s*[:=]?\s*([\d\.,]+)",
    ])

    if total_amount is None:
        amounts = []
        for m in re.finditer(r"([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2}|[0-9]+,[0-9]{2})", text_value):
            amt = parse_amount_es(m.group(1))
            if amt is not None:
                amounts.append(abs(float(amt)))
        if amounts:
            total_amount = round(max(amounts), 2)
            result["warnings"].append("El total se ha estimado tomando el mayor importe detectado.")

    if base_amount is None and total_amount is not None and tax_amount is not None:
        base_amount = round(total_amount - tax_amount, 2)
    if tax_amount is None and base_amount is not None and total_amount is not None:
        tax_amount = round(total_amount - base_amount, 2)

    tax_rate = 21.0
    rate_raw = _extract_first([r"iva\s*(\d{1,2}(?:[\.,]\d{1,2})?)\s*%"], text_value)
    if rate_raw:
        try:
            tax_rate = float(str(rate_raw).replace(",", "."))
        except Exception:
            pass
    elif base_amount not in (None, 0) and tax_amount is not None:
        try:
            tax_rate = round((float(tax_amount) / float(base_amount)) * 100.0, 2)
        except Exception:
            pass

    result["base_amount"] = round(float(base_amount or 0.0), 2)
    result["tax_amount"] = round(float(tax_amount or 0.0), 2)
    result["total_amount"] = round(float(total_amount or 0.0), 2)
    result["tax_rate"] = round(float(tax_rate or 21.0), 2)

    # Match supplier against internal DB
    sdf = suppliers_df(active_only=False)
    if not sdf.empty:
        text_norm = _norm(text_value)
        text_tax = _clean_tax_id(text_value)
        best = None
        best_score = 0
        for _, row in sdf.iterrows():
            score = 0
            tax_id = _clean_tax_id(row.get("tax_id", ""))
            name = str(row.get("name", "") or "").strip()
            name_norm = _norm(name)
            if tax_id and tax_id in text_tax:
                score += 10
            if name_norm and len(name_norm) >= 5 and name_norm in text_norm:
                score += 5
            if score > best_score:
                best_score = score
                best = row
        if best is not None and best_score > 0:
            result["supplier_id"] = int(best["id"])
            result["supplier_match_label"] = f"{int(best['id'])} · {best['name']}"
        else:
            result["warnings"].append("No se pudo identificar automáticamente el proveedor en tu base de datos.")

    if result["base_amount"] <= 0 and result["total_amount"] <= 0:
        result["warnings"].append("No se detectaron importes con suficiente fiabilidad. Revisa los campos antes de guardar.")

    return result


def parse_amount_es(s: str):
    s = (s or "").strip()
    s = s.replace("€", "").replace("EUR", "").replace(" ", "")

    sign = -1 if (s.startswith("(") and s.endswith(")")) else 1
    s = s.strip("()")

    if s.endswith("-"):
        sign *= -1
        s = s[:-1]
    if s.startswith("-"):
        sign *= -1
        s = s[1:]

    if not re.search(r"\d", s):
        return None

    s = s.replace(".", "").replace(",", ".")
    try:
        return sign * float(s)
    except ValueError:
        return None


def spot_lines_concept_amount(text_block: str, row_type: str) -> list[dict]:
    rows = []
    for line in text_block.splitlines():
        line = line.strip()
        if len(line) < 6:
            continue
        m = re.search(r"(.+?)\s+(-?\(?\d[\d\.\,]*\)?-?)(?:\s*€)?$", line)
        if not m:
            continue
        concept = m.group(1).strip(" .-:\t")
        amt = parse_amount_es(m.group(2))
        if amt is None or len(concept) < 4:
            continue
        rows.append({
            "row_type": row_type,
            "row_key": concept[:120],
            "description": concept[:240],
            "amount": amt,
            "extra": {"source": "regex_line"},
        })
    return rows


def spot_bank_extract(text_block: str) -> list[dict]:
    rows = []
    date_pat = r"(\d{2}[\/\-]\d{2}[\/\-]\d{2,4})"
    for line in text_block.splitlines():
        line = line.strip()
        m = re.search(date_pat + r"\s+(.+?)\s+(-?\(?\d[\d\.\,]*\)?-?)(?:\s*€)?$", line)
        if not m:
            continue
        dt_raw = m.group(1)
        desc = m.group(2).strip()
        amt = parse_amount_es(m.group(3))
        if amt is None:
            continue
        rows.append({
            "row_type": "bank_txn",
            "row_date": dt_raw,
            "description": desc[:240],
            "amount": amt,
            "extra": {"source": "regex_line", "raw_date": dt_raw},
        })

    m2 = re.search(r"Saldo\s+final.*?:\s*(-?\(?\d[\d\.\,]*\)?-?)(?:\s*€)?", text_block, re.IGNORECASE)
    if m2:
        saldo = parse_amount_es(m2.group(1))
        if saldo is not None:
            rows.append({
                "row_type": "bank_balance",
                "row_key": "saldo_final_3112",
                "description": "Saldo final a 31/12",
                "amount": saldo,
                "extra": {"source": "regex_block"},
            })
    return rows


def spot_document(doc_type: str, filepath: str) -> tuple[list[dict], str]:
    try:
        txt = pdf_to_text(filepath)
        if not txt:
            return [], "No se pudo extraer texto (posible PDF escaneado)."

        if doc_type == "PYG_ANUAL":
            rows = spot_lines_concept_amount(txt, "pnl_line")
        elif doc_type == "BALANCE_ANUAL":
            rows = spot_lines_concept_amount(txt, "balance_line")
        elif doc_type == "EXTRACTO_CIERRE":
            rows = spot_bank_extract(txt)
        elif doc_type == "COSTES_LABORALES_ANUAL":
            rows = spot_lines_concept_amount(txt, "labor_cost_line")
        elif doc_type == "PRESTAMOS_DEUDAS_CIERRE":
            rows = spot_lines_concept_amount(txt, "loan_line")
        else:
            rows = []

        return rows, f"Texto extraído OK. Filas detectadas: {len(rows)}"
    except Exception as e:
        return [], f"Error al procesar PDF: {e}"


# =======================
# MODELS + SIGNED SALES
# =======================
def models_df(active_only=True):
    q = "SELECT id, code, name, fuel, list_price, active FROM models"
    if active_only:
        q += " WHERE active=1"
    q += " ORDER BY fuel, code"
    return read_df(q)


def add_signed_sale(created_by: str, year: int, period: str, customer: str, model_id: int, units: int, unit_price: float, notes: str):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO signed_sales(created_at, created_by, year, period, customer, model_id, units, unit_price, notes, status)
            VALUES(:at,:by,:y,:p,:c,:mid,:u,:pr,:n,'signed')
        """), {
            "at": datetime.now().isoformat(timespec="seconds"),
            "by": created_by,
            "y": int(year),
            "p": period,
            "c": customer.strip(),
            "mid": int(model_id),
            "u": int(units),
            "pr": float(unit_price),
            "n": (notes or "").strip(),
        })


def signed_sales_df(year: int, viewer_role: str, viewer_user: str) -> pd.DataFrame:
    base = """
        SELECT s.id, s.created_at, s.created_by AS comercial, s.year, s.period, s.customer,
               m.code AS modelo, m.fuel AS combustible,
               s.units, s.unit_price,
               ROUND(s.units*s.unit_price, 2) AS total,
               s.status, s.notes
        FROM signed_sales s
        LEFT JOIN models m ON m.id = s.model_id
        WHERE s.year=:y AND s.status='signed'
    """
    params = {"y": int(year)}
    if viewer_role == ROLE_COMMERCIAL:
        base += " AND s.created_by=:u"
        params["u"] = viewer_user

    base += " ORDER BY s.period ASC, s.created_at DESC"
    return read_df(base, params)


def signed_sales_monthly_summary(year: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = signed_sales_df(year, ROLE_OWNER, "owner")  # admin view for summaries
    if df.empty:
        return df, pd.DataFrame(), pd.DataFrame()
    monthly = df.groupby("period", as_index=False)["total"].sum().sort_values("period")
    by_salesperson = df.groupby("comercial", as_index=False)["total"].sum().sort_values("total", ascending=False)
    return df, monthly, by_salesperson


# =======================
# EXPENSE FORECAST HELPERS
# =======================
DEFAULT_EXPENSE_CATEGORIES = [
    "Nóminas",
    "Seguridad Social",
    "Alquiler",
    "Suministros",
    "Marketing",
    "Transporte",
    "Software",
    "Servicios profesionales",
    "Financiación/Intereses",
    "Impuestos",
    "Otros",
]


def expense_forecast_df(year: int) -> pd.DataFrame:
    return read_df(
        """
        SELECT id, year, period, category, amount, notes, created_at, created_by, updated_at, updated_by
        FROM expense_forecast
        WHERE year=:y
        ORDER BY period ASC, category ASC
        """,
        {"y": int(year)},
    )


def expense_forecast_monthly_summary(year: int) -> pd.DataFrame:
    df = expense_forecast_df(year)
    if df.empty:
        return pd.DataFrame(columns=["period", "expenses"])
    out = df.groupby("period", as_index=False)["amount"].sum().rename(columns={"amount": "expenses"})
    out["expenses"] = out["expenses"].astype(float)
    return out.sort_values("period")


def upsert_expense_forecast(year: int, period: str, category: str, amount: float, notes: str, actor: str):
    with engine.begin() as conn:
        existing = conn.execute(
            text("SELECT id FROM expense_forecast WHERE year=:y AND period=:p AND category=:c"),
            {"y": int(year), "p": period, "c": category},
        ).fetchone()
        now = datetime.now().isoformat(timespec="seconds")
        if existing:
            conn.execute(
                text("""
                    UPDATE expense_forecast
                    SET amount=:a, notes=:n, updated_at=:ua, updated_by=:ub
                    WHERE id=:id
                """),
                {"a": float(amount), "n": (notes or "").strip(), "ua": now, "ub": actor, "id": int(existing[0])},
            )
        else:
            conn.execute(
                text("""
                    INSERT INTO expense_forecast(year, period, category, amount, notes, created_at, created_by)
                    VALUES(:y,:p,:c,:a,:n,:ca,:cb)
                """),
                {"y": int(year), "p": period, "c": category, "a": float(amount), "n": (notes or "").strip(), "ca": now, "cb": actor},
            )


def delete_expense_forecast(row_id: int):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM expense_forecast WHERE id=:id"), {"id": int(row_id)})


def copy_expense_budget(from_year: int, to_year: int, actor: str, overwrite: bool = False):
    src = expense_forecast_df(from_year)
    if src.empty:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    count = 0
    with engine.begin() as conn:
        for _, r in src.iterrows():
            period = str(r["period"]).replace(str(from_year), str(to_year), 1)
            if overwrite:
                conn.execute(
                    text("DELETE FROM expense_forecast WHERE year=:y AND period=:p AND category=:c"),
                    {"y": int(to_year), "p": period, "c": str(r["category"])},
                )
            conn.execute(
                text("""
                    INSERT OR IGNORE INTO expense_forecast(year, period, category, amount, notes, created_at, created_by)
                    VALUES(:y,:p,:c,:a,:n,:ca,:cb)
                """),
                {
                    "y": int(to_year),
                    "p": period,
                    "c": str(r["category"]),
                    "a": float(r["amount"]),
                    "n": str(r["notes"] or ""),
                    "ca": now,
                    "cb": actor,
                },
            )
            count += 1
    return count


# =======================
# CUSTOMERS / SUPPLIERS / SYNC QUEUE
# =======================
def _clean_text(value) -> str:
    return str(value or "").strip()


def _clean_upper(value) -> str:
    return _clean_text(value).upper()


def _entity_payload(name: str, tax_id: str, email: str, phone: str, address: str, city: str, postal_code: str, province: str, country: str, notes: str, active: bool) -> dict:
    return {
        "name": _clean_text(name),
        "tax_id": _clean_upper(tax_id),
        "email": _clean_text(email),
        "phone": _clean_text(phone),
        "address": _clean_text(address),
        "city": _clean_text(city),
        "postal_code": _clean_text(postal_code),
        "province": _clean_text(province),
        "country": _clean_upper(country or "ES") or "ES",
        "notes": _clean_text(notes),
        "active": 1 if active else 0,
    }


def customers_df(active_only: bool = False) -> pd.DataFrame:
    q = """
        SELECT id, name, tax_id, email, phone, city, province, country, active,
               external_id_mygestion, sync_status, sync_error, created_at, created_by, updated_at, updated_by
        FROM customers
    """
    if active_only:
        q += " WHERE active=1"
    q += " ORDER BY active DESC, name ASC"
    return read_df(q)


def suppliers_df(active_only: bool = False) -> pd.DataFrame:
    q = """
        SELECT id, name, tax_id, email, phone, city, province, country, active,
               external_id_mygestion, sync_status, sync_error, created_at, created_by, updated_at, updated_by
        FROM suppliers
    """
    if active_only:
        q += " WHERE active=1"
    q += " ORDER BY active DESC, name ASC"
    return read_df(q)


def get_customer(customer_id: int):
    with engine.begin() as conn:
        row = conn.execute(text("SELECT * FROM customers WHERE id=:id"), {"id": int(customer_id)}).mappings().first()
    return dict(row) if row else None


def get_supplier(supplier_id: int):
    with engine.begin() as conn:
        row = conn.execute(text("SELECT * FROM suppliers WHERE id=:id"), {"id": int(supplier_id)}).mappings().first()
    return dict(row) if row else None


def enqueue_sync(entity_type: str, entity_id: int, action: str, payload: dict):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sync_queue(entity_type, entity_id, action, status, attempts, last_error, payload_json, created_at, processed_at)
            VALUES(:et,:eid,:ac,'pending',0,NULL,:pl,:ca,NULL)
        """), {
            "et": entity_type,
            "eid": int(entity_id),
            "ac": action,
            "pl": json.dumps(payload, ensure_ascii=False),
            "ca": datetime.now().isoformat(timespec="seconds"),
        })


def sync_queue_df(status: str | None = None, limit: int = 200) -> pd.DataFrame:
    q = """
        SELECT id, entity_type, entity_id, action, status, attempts, last_error, payload_json, created_at, processed_at
        FROM sync_queue
    """
    params = {"lim": int(limit)}
    if status and status != "all":
        q += " WHERE status=:st"
        params["st"] = status
    q += " ORDER BY created_at DESC, id DESC LIMIT :lim"
    return read_df(q, params)


def update_sync_status(queue_id: int, status: str, error: str = ""):
    processed_at = datetime.now().isoformat(timespec="seconds") if status in ("done", "error") else None
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE sync_queue
            SET status=:st,
                attempts=attempts + 1,
                last_error=:err,
                processed_at=:pa
            WHERE id=:id
        """), {"st": status, "err": _clean_text(error) or None, "pa": processed_at, "id": int(queue_id)})


def reset_sync_item(queue_id: int):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE sync_queue
            SET status='pending', last_error=NULL, processed_at=NULL
            WHERE id=:id
        """), {"id": int(queue_id)})


def add_customer(actor: str, name: str, tax_id: str = "", email: str = "", phone: str = "", address: str = "", city: str = "", postal_code: str = "", province: str = "", country: str = "ES", notes: str = "", active: bool = True) -> int:
    payload = _entity_payload(name, tax_id, email, phone, address, city, postal_code, province, country, notes, active)
    if not payload["name"]:
        raise ValueError("El nombre del cliente es obligatorio.")
    now = datetime.now().isoformat(timespec="seconds")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO customers(name, tax_id, email, phone, address, city, postal_code, province, country, notes, active, external_id_mygestion, sync_status, sync_error, created_at, created_by, updated_at, updated_by)
            VALUES(:name,:tax_id,:email,:phone,:address,:city,:postal_code,:province,:country,:notes,:active,NULL,'pending',NULL,:created_at,:created_by,NULL,NULL)
        """), {**payload, "created_at": now, "created_by": actor})
        row_id = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()["id"]
    enqueue_sync("customer", int(row_id), "create", payload)
    return int(row_id)


def add_supplier(actor: str, name: str, tax_id: str = "", email: str = "", phone: str = "", address: str = "", city: str = "", postal_code: str = "", province: str = "", country: str = "ES", notes: str = "", active: bool = True) -> int:
    payload = _entity_payload(name, tax_id, email, phone, address, city, postal_code, province, country, notes, active)
    if not payload["name"]:
        raise ValueError("El nombre del proveedor es obligatorio.")
    now = datetime.now().isoformat(timespec="seconds")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO suppliers(name, tax_id, email, phone, address, city, postal_code, province, country, notes, active, external_id_mygestion, sync_status, sync_error, created_at, created_by, updated_at, updated_by)
            VALUES(:name,:tax_id,:email,:phone,:address,:city,:postal_code,:province,:country,:notes,:active,NULL,'pending',NULL,:created_at,:created_by,NULL,NULL)
        """), {**payload, "created_at": now, "created_by": actor})
        row_id = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()["id"]
    enqueue_sync("supplier", int(row_id), "create", payload)
    return int(row_id)


def update_customer(customer_id: int, actor: str, name: str, tax_id: str = "", email: str = "", phone: str = "", address: str = "", city: str = "", postal_code: str = "", province: str = "", country: str = "ES", notes: str = "", active: bool = True):
    payload = _entity_payload(name, tax_id, email, phone, address, city, postal_code, province, country, notes, active)
    if not payload["name"]:
        raise ValueError("El nombre del cliente es obligatorio.")
    now = datetime.now().isoformat(timespec="seconds")
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE customers
            SET name=:name, tax_id=:tax_id, email=:email, phone=:phone, address=:address, city=:city, postal_code=:postal_code, province=:province, country=:country, notes=:notes, active=:active,
                sync_status='pending', sync_error=NULL, updated_at=:updated_at, updated_by=:updated_by
            WHERE id=:id
        """), {**payload, "updated_at": now, "updated_by": actor, "id": int(customer_id)})
    enqueue_sync("customer", int(customer_id), "update", payload)


def update_supplier(supplier_id: int, actor: str, name: str, tax_id: str = "", email: str = "", phone: str = "", address: str = "", city: str = "", postal_code: str = "", province: str = "", country: str = "ES", notes: str = "", active: bool = True):
    payload = _entity_payload(name, tax_id, email, phone, address, city, postal_code, province, country, notes, active)
    if not payload["name"]:
        raise ValueError("El nombre del proveedor es obligatorio.")
    now = datetime.now().isoformat(timespec="seconds")
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE suppliers
            SET name=:name, tax_id=:tax_id, email=:email, phone=:phone, address=:address, city=:city, postal_code=:postal_code, province=:province, country=:country, notes=:notes, active=:active,
                sync_status='pending', sync_error=NULL, updated_at=:updated_at, updated_by=:updated_by
            WHERE id=:id
        """), {**payload, "updated_at": now, "updated_by": actor, "id": int(supplier_id)})
    enqueue_sync("supplier", int(supplier_id), "update", payload)


def customer_options() -> pd.DataFrame:
    return read_df("""
        SELECT id, name, tax_id, email, phone, city, province, active
        FROM customers
        WHERE active=1
        ORDER BY name
    """)


def supplier_options() -> pd.DataFrame:
    return read_df("""
        SELECT id, name, tax_id, email, phone, city, province, active
        FROM suppliers
        WHERE active=1
        ORDER BY name
    """)


def sales_invoices_df(limit: int = 500) -> pd.DataFrame:
    return read_df("""
        SELECT si.id, si.invoice_number, si.invoice_date, si.customer_id,
               c.name AS customer_name,
               si.base_amount, si.tax_rate, si.tax_amount, si.total_amount,
               si.status, si.external_id_mygestion, si.sync_status, si.sync_error,
               si.created_at, si.created_by, si.updated_at, si.updated_by
        FROM sales_invoices si
        JOIN customers c ON c.id = si.customer_id
        ORDER BY si.invoice_date DESC, si.id DESC
        LIMIT :limit
    """, {"limit": int(limit)})


def get_sales_invoice(invoice_id: int) -> dict | None:
    with engine.begin() as conn:
        r = conn.execute(text("""
            SELECT id, invoice_number, invoice_date, customer_id, base_amount, tax_rate,
                   tax_amount, total_amount, notes, status, external_id_mygestion,
                   sync_status, sync_error, created_at, created_by, updated_at, updated_by
            FROM sales_invoices
            WHERE id=:id
        """), {"id": int(invoice_id)}).mappings().first()
    return dict(r) if r else None


def _invoice_payload(invoice_number: str, invoice_date: str, customer_id: int, base_amount: float,
                     tax_rate: float, notes: str = "", status: str = "draft") -> dict:
    base = round(float(base_amount), 2)
    rate = round(float(tax_rate), 2)
    tax = round(base * rate / 100.0, 2)
    total = round(base + tax, 2)
    return {
        "invoice_number": (invoice_number or "").strip(),
        "invoice_date": str(invoice_date),
        "customer_id": int(customer_id),
        "base_amount": base,
        "tax_rate": rate,
        "tax_amount": tax,
        "total_amount": total,
        "notes": (notes or "").strip(),
        "status": (status or "draft").strip() or "draft",
    }


def add_sales_invoice(actor: str, invoice_number: str, invoice_date: str, customer_id: int,
                      base_amount: float, tax_rate: float = 21.0, notes: str = "",
                      status: str = "draft") -> int:
    payload = _invoice_payload(invoice_number, invoice_date, customer_id, base_amount, tax_rate, notes, status)
    if not payload["invoice_number"]:
        raise ValueError("El número de factura es obligatorio.")
    now = datetime.now().isoformat(timespec="seconds")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO sales_invoices(
                invoice_number, invoice_date, customer_id, base_amount, tax_rate, tax_amount, total_amount,
                notes, status, external_id_mygestion, sync_status, sync_error,
                created_at, created_by, updated_at, updated_by
            ) VALUES(
                :invoice_number, :invoice_date, :customer_id, :base_amount, :tax_rate, :tax_amount, :total_amount,
                :notes, :status, NULL, 'pending', NULL,
                :created_at, :created_by, NULL, NULL
            )
        """), {**payload, "created_at": now, "created_by": actor})
        invoice_id = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()["id"]
    enqueue_sync("sales_invoice", int(invoice_id), "create", payload)
    return int(invoice_id)


def update_sales_invoice(invoice_id: int, actor: str, invoice_number: str, invoice_date: str, customer_id: int,
                         base_amount: float, tax_rate: float = 21.0, notes: str = "", status: str = "draft"):
    payload = _invoice_payload(invoice_number, invoice_date, customer_id, base_amount, tax_rate, notes, status)
    if not payload["invoice_number"]:
        raise ValueError("El número de factura es obligatorio.")
    now = datetime.now().isoformat(timespec="seconds")
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE sales_invoices
            SET invoice_number=:invoice_number, invoice_date=:invoice_date, customer_id=:customer_id,
                base_amount=:base_amount, tax_rate=:tax_rate, tax_amount=:tax_amount, total_amount=:total_amount,
                notes=:notes, status=:status,
                sync_status='pending', sync_error=NULL,
                updated_at=:updated_at, updated_by=:updated_by
            WHERE id=:id
        """), {**payload, "updated_at": now, "updated_by": actor, "id": int(invoice_id)})
    enqueue_sync("sales_invoice", int(invoice_id), "update", payload)


def _default_invoice_number() -> str:
    y = date.today().year
    prefix = f"F{y}-"
    with engine.begin() as conn:
        r = conn.execute(text("""
            SELECT invoice_number
            FROM sales_invoices
            WHERE invoice_number LIKE :prefix
            ORDER BY id DESC
            LIMIT 1
        """), {"prefix": f"{prefix}%"}).mappings().first()
    if not r or not r.get("invoice_number"):
        return f"{prefix}0001"
    m = re.search(r"(\d+)$", str(r["invoice_number"]))
    nxt = int(m.group(1)) + 1 if m else 1
    return f"{prefix}{nxt:04d}"


def render_sales_invoices_admin(actor: str):
    st.subheader("Administrador · Facturas emitidas")
    st.caption("Alta en base propia + cola de sincronización a myGestión.")

    cdf = customer_options()
    if cdf.empty:
        st.warning("Primero debes crear al menos un cliente activo.")
        return

    customer_labels = {f"{int(r['id'])} · {r['name']}": int(r['id']) for _, r in cdf.iterrows()}
    customer_label_list = list(customer_labels.keys())

    tab1, tab2, tab3 = st.tabs(["Alta manual", "Alta desde PDF", "Editar / consultar"])

    with tab1:
        with st.form("form_add_sales_invoice", clear_on_submit=False):
            c1, c2, c3 = st.columns(3)
            invoice_number = c1.text_input("Número de factura", value=_default_invoice_number())
            invoice_date = c2.date_input("Fecha factura", value=date.today())
            customer_label = c3.selectbox("Cliente", options=customer_label_list, index=0, key="sales_invoice_customer_new")

            c4, c5, c6 = st.columns(3)
            base_amount = c4.number_input("Base imponible (€)", min_value=0.0, step=10.0, value=0.0)
            tax_rate = c5.number_input("IVA (%)", min_value=0.0, max_value=100.0, step=1.0, value=21.0)
            status = c6.selectbox("Estado", options=["draft", "issued", "cancelled"], index=0, key="sales_invoice_status_new")

            preview_tax = round(float(base_amount) * float(tax_rate) / 100.0, 2)
            preview_total = round(float(base_amount) + preview_tax, 2)
            st.caption(f"IVA calculado: {preview_tax:.2f} € · Total: {preview_total:.2f} €")
            notes = st.text_area("Notas", height=90)
            submitted = st.form_submit_button("Guardar factura emitida")

        if submitted:
            try:
                invoice_id = add_sales_invoice(
                    actor=actor,
                    invoice_number=invoice_number,
                    invoice_date=invoice_date.isoformat() if hasattr(invoice_date, 'isoformat') else str(invoice_date),
                    customer_id=customer_labels[customer_label],
                    base_amount=base_amount,
                    tax_rate=tax_rate,
                    notes=notes,
                    status=status,
                )
                st.success(f"Factura emitida guardada con ID interno {invoice_id}. Pendiente de sincronización.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar la factura: {e}")

    with tab2:
        df = sales_invoices_df(limit=500)
        if df.empty:
            st.info("Todavía no hay facturas emitidas.")
            return
        st.dataframe(df, use_container_width=True, hide_index=True)
        invoice_options = [f"{int(r['id'])} · {r['invoice_number']} · {r['customer_name']}" for _, r in df.iterrows()]
        selected = st.selectbox("Selecciona factura emitida", options=invoice_options, index=0, key="sales_invoice_edit_select")
        invoice_id = int(selected.split("·", 1)[0].strip())
        current = get_sales_invoice(invoice_id)
        current_customer_label = next((label for label, cid in customer_labels.items() if cid == int(current["customer_id"])), customer_label_list[0])

        with st.form(f"form_edit_sales_invoice_{invoice_id}", clear_on_submit=False):
            c1, c2, c3 = st.columns(3)
            invoice_number = c1.text_input("Número de factura", value=str(current.get("invoice_number") or ""))
            current_date = pd.to_datetime(current.get("invoice_date")).date() if current.get("invoice_date") else date.today()
            invoice_date = c2.date_input("Fecha factura", value=current_date, key=f"sales_invoice_date_{invoice_id}")
            customer_label = c3.selectbox(
                "Cliente",
                options=customer_label_list,
                index=customer_label_list.index(current_customer_label) if current_customer_label in customer_label_list else 0,
                key=f"sales_invoice_customer_{invoice_id}",
            )

            c4, c5, c6 = st.columns(3)
            base_amount = c4.number_input("Base imponible (€)", min_value=0.0, step=10.0, value=float(current.get("base_amount") or 0.0), key=f"sales_invoice_base_{invoice_id}")
            tax_rate = c5.number_input("IVA (%)", min_value=0.0, max_value=100.0, step=1.0, value=float(current.get("tax_rate") or 21.0), key=f"sales_invoice_tax_{invoice_id}")
            status = c6.selectbox("Estado", options=["draft", "issued", "cancelled"], index=["draft", "issued", "cancelled"].index(str(current.get("status") or "draft")) if str(current.get("status") or "draft") in ["draft", "issued", "cancelled"] else 0, key=f"sales_invoice_status_{invoice_id}")

            preview_tax = round(float(base_amount) * float(tax_rate) / 100.0, 2)
            preview_total = round(float(base_amount) + preview_tax, 2)
            st.caption(f"IVA calculado: {preview_tax:.2f} € · Total: {preview_total:.2f} €")
            notes = st.text_area("Notas", value=str(current.get("notes") or ""), height=90, key=f"sales_invoice_notes_{invoice_id}")
            submitted = st.form_submit_button("Actualizar factura emitida")

        if submitted:
            try:
                update_sales_invoice(
                    invoice_id=invoice_id,
                    actor=actor,
                    invoice_number=invoice_number,
                    invoice_date=invoice_date.isoformat() if hasattr(invoice_date, 'isoformat') else str(invoice_date),
                    customer_id=customer_labels[customer_label],
                    base_amount=base_amount,
                    tax_rate=tax_rate,
                    notes=notes,
                    status=status,
                )
                st.success("Factura emitida actualizada. Se ha añadido un evento a la cola de sincronización.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo actualizar la factura: {e}")


def _render_partner_form(entity_label: str, form_key: str, submit_label: str, defaults=None):
    defaults = defaults or {}
    with st.form(form_key, clear_on_submit=False):
        c1, c2 = st.columns(2)
        name = c1.text_input(f"Nombre / razón social ({entity_label})", value=defaults.get("name", ""))
        tax_id = c2.text_input("NIF/CIF", value=defaults.get("tax_id", ""))

        c3, c4 = st.columns(2)
        email = c3.text_input("Email", value=defaults.get("email", ""))
        phone = c4.text_input("Teléfono", value=defaults.get("phone", ""))

        address = st.text_input("Dirección", value=defaults.get("address", ""))

        c5, c6, c7, c8 = st.columns(4)
        city = c5.text_input("Ciudad", value=defaults.get("city", ""))
        postal_code = c6.text_input("Código postal", value=defaults.get("postal_code", ""))
        province = c7.text_input("Provincia", value=defaults.get("province", ""))
        country = c8.text_input("País", value=defaults.get("country", "ES"))

        notes = st.text_area("Notas", value=defaults.get("notes", ""), height=80)
        active = st.checkbox("Activo", value=bool(defaults.get("active", 1)))
        submitted = st.form_submit_button(submit_label)

    return submitted, {
        "name": name,
        "tax_id": tax_id,
        "email": email,
        "phone": phone,
        "address": address,
        "city": city,
        "postal_code": postal_code,
        "province": province,
        "country": country,
        "notes": notes,
        "active": active,
    }


def render_customers_admin(actor: str):
    st.subheader("Administrador · Clientes")
    st.caption("Alta en base de datos propia + creación de cola de sincronización para myGestión.")

    tab1, tab2, tab3 = st.tabs(["Alta manual", "Alta desde PDF", "Editar / consultar"])

    with tab1:
        submitted, payload = _render_partner_form("cliente", "form_add_customer", "Guardar cliente")
        if submitted:
            try:
                customer_id = add_customer(actor=actor, **payload)
                st.success(f"Cliente guardado con ID interno {customer_id}. Pendiente de sincronización.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar el cliente: {e}")

    with tab2:
        df = customers_df(active_only=False)
        if df.empty:
            st.info("Todavía no hay clientes.")
            return
        st.dataframe(df, use_container_width=True, hide_index=True)
        options = [f"{int(r['id'])} · {r['name']}" for _, r in df.iterrows()]
        selected = st.selectbox("Selecciona cliente", options=options, index=0)
        customer_id = int(selected.split("·", 1)[0].strip())
        current = get_customer(customer_id)
        submitted, payload = _render_partner_form("cliente", f"form_edit_customer_{customer_id}", "Actualizar cliente", current)
        if submitted:
            try:
                update_customer(customer_id=customer_id, actor=actor, **payload)
                st.success("Cliente actualizado. Se ha añadido un evento a la cola de sincronización.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo actualizar el cliente: {e}")


def render_suppliers_admin(actor: str):
    st.subheader("Administrador · Proveedores")
    st.caption("Alta en base de datos propia + creación de cola de sincronización para myGestión.")

    tab1, tab2, tab3 = st.tabs(["Alta manual", "Alta desde PDF", "Editar / consultar"])

    with tab1:
        submitted, payload = _render_partner_form("proveedor", "form_add_supplier", "Guardar proveedor")
        if submitted:
            try:
                supplier_id = add_supplier(actor=actor, **payload)
                st.success(f"Proveedor guardado con ID interno {supplier_id}. Pendiente de sincronización.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo guardar el proveedor: {e}")

    with tab2:
        df = suppliers_df(active_only=False)
        if df.empty:
            st.info("Todavía no hay proveedores.")
            return
        st.dataframe(df, use_container_width=True, hide_index=True)
        options = [f"{int(r['id'])} · {r['name']}" for _, r in df.iterrows()]
        selected = st.selectbox("Selecciona proveedor", options=options, index=0)
        supplier_id = int(selected.split("·", 1)[0].strip())
        current = get_supplier(supplier_id)
        submitted, payload = _render_partner_form("proveedor", f"form_edit_supplier_{supplier_id}", "Actualizar proveedor", current)
        if submitted:
            try:
                update_supplier(supplier_id=supplier_id, actor=actor, **payload)
                st.success("Proveedor actualizado. Se ha añadido un evento a la cola de sincronización.")
                st.rerun()
            except Exception as e:
                st.error(f"No se pudo actualizar el proveedor: {e}")




def purchase_invoices_df(limit: int = 500) -> pd.DataFrame:
    return read_df("""
        SELECT pi.id, pi.supplier_invoice_number, pi.invoice_date, pi.supplier_id,
               s.name AS supplier_name,
               pi.base_amount, pi.tax_rate, pi.tax_amount, pi.total_amount, pi.deductible,
               pi.status, pi.attachment_path, pi.attachment_name, pi.attachment_mime,
               pi.external_id_mygestion, pi.sync_status, pi.sync_error,
               pi.created_at, pi.created_by, pi.updated_at, pi.updated_by
        FROM purchase_invoices pi
        JOIN suppliers s ON s.id = pi.supplier_id
        ORDER BY pi.invoice_date DESC, pi.id DESC
        LIMIT :limit
    """, {"limit": int(limit)})


def get_purchase_invoice(invoice_id: int) -> dict | None:
    with engine.begin() as conn:
        r = conn.execute(text("""
            SELECT id, supplier_invoice_number, invoice_date, supplier_id, base_amount, tax_rate,
                   tax_amount, total_amount, deductible, notes, status, attachment_path, attachment_name,
                   attachment_mime, attachment_uploaded_at, attachment_uploaded_by, external_id_mygestion,
                   sync_status, sync_error, created_at, created_by, updated_at, updated_by
            FROM purchase_invoices
            WHERE id=:id
        """), {"id": int(invoice_id)}).mappings().first()
    return dict(r) if r else None


def _purchase_invoice_payload(supplier_invoice_number: str, invoice_date: str, supplier_id: int, base_amount: float,
                              tax_rate: float, deductible: bool = True, notes: str = "", status: str = "draft") -> dict:
    base = round(float(base_amount), 2)
    rate = round(float(tax_rate), 2)
    tax = round(base * rate / 100.0, 2)
    total = round(base + tax, 2)
    return {
        "supplier_invoice_number": (supplier_invoice_number or "").strip(),
        "invoice_date": str(invoice_date),
        "supplier_id": int(supplier_id),
        "base_amount": base,
        "tax_rate": rate,
        "tax_amount": tax,
        "total_amount": total,
        "deductible": 1 if bool(deductible) else 0,
        "notes": (notes or "").strip(),
        "status": (status or "draft").strip() or "draft",
    }


def add_purchase_invoice(actor: str, supplier_invoice_number: str, invoice_date: str, supplier_id: int,
                         base_amount: float, tax_rate: float = 21.0, deductible: bool = True,
                         notes: str = "", status: str = "draft") -> int:
    payload = _purchase_invoice_payload(supplier_invoice_number, invoice_date, supplier_id, base_amount, tax_rate, deductible, notes, status)
    if not payload["supplier_invoice_number"]:
        raise ValueError("El número de factura del proveedor es obligatorio.")
    now = datetime.now().isoformat(timespec="seconds")
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO purchase_invoices(
                supplier_invoice_number, invoice_date, supplier_id, base_amount, tax_rate, tax_amount, total_amount,
                deductible, notes, status, external_id_mygestion, sync_status, sync_error,
                created_at, created_by, updated_at, updated_by
            ) VALUES(
                :supplier_invoice_number, :invoice_date, :supplier_id, :base_amount, :tax_rate, :tax_amount, :total_amount,
                :deductible, :notes, :status, NULL, 'pending', NULL,
                :created_at, :created_by, NULL, NULL
            )
        """), {**payload, "created_at": now, "created_by": actor})
        invoice_id = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()["id"]
    enqueue_sync("purchase_invoice", int(invoice_id), "create", payload)
    return int(invoice_id)


def update_purchase_invoice(invoice_id: int, actor: str, supplier_invoice_number: str, invoice_date: str, supplier_id: int,
                            base_amount: float, tax_rate: float = 21.0, deductible: bool = True,
                            notes: str = "", status: str = "draft"):
    payload = _purchase_invoice_payload(supplier_invoice_number, invoice_date, supplier_id, base_amount, tax_rate, deductible, notes, status)
    if not payload["supplier_invoice_number"]:
        raise ValueError("El número de factura del proveedor es obligatorio.")
    now = datetime.now().isoformat(timespec="seconds")
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE purchase_invoices
            SET supplier_invoice_number=:supplier_invoice_number, invoice_date=:invoice_date, supplier_id=:supplier_id,
                base_amount=:base_amount, tax_rate=:tax_rate, tax_amount=:tax_amount, total_amount=:total_amount,
                deductible=:deductible, notes=:notes, status=:status,
                sync_status='pending', sync_error=NULL,
                updated_at=:updated_at, updated_by=:updated_by
            WHERE id=:id
        """), {**payload, "updated_at": now, "updated_by": actor, "id": int(invoice_id)})
    enqueue_sync("purchase_invoice", int(invoice_id), "update", payload)


def _safe_filename(name: str) -> str:
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(name or "archivo"))
    return name.strip("._") or "archivo"


def save_purchase_invoice_attachment(invoice_id: int, uploaded_file, actor: str) -> str:
    if uploaded_file is None:
        raise ValueError("Debes seleccionar un archivo.")
    invoice_id = int(invoice_id)
    original_name = getattr(uploaded_file, "name", "documento.pdf") or "documento.pdf"
    safe_name = _safe_filename(original_name)
    suffix = Path(safe_name).suffix or ".bin"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    target_dir = ATTACHMENTS_DIR / f"purchase_invoice_{invoice_id}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{ts}_{safe_name}"
    data = uploaded_file.getbuffer() if hasattr(uploaded_file, "getbuffer") else uploaded_file.read()
    with open(target_path, "wb") as f:
        f.write(data)
    mime = getattr(uploaded_file, "type", None) or "application/octet-stream"
    now = datetime.now().isoformat(timespec="seconds")
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE purchase_invoices
            SET attachment_path=:path, attachment_name=:name, attachment_mime=:mime,
                attachment_uploaded_at=:uploaded_at, attachment_uploaded_by=:uploaded_by,
                updated_at=:updated_at, updated_by=:updated_by
            WHERE id=:id
        """), {
            "path": str(target_path),
            "name": original_name,
            "mime": mime,
            "uploaded_at": now,
            "uploaded_by": actor,
            "updated_at": now,
            "updated_by": actor,
            "id": invoice_id,
        })
    return str(target_path)


def attachment_download_name(current: dict) -> str:
    return str(current.get("attachment_name") or Path(str(current.get("attachment_path") or "adjunto")).name)


def attachment_exists(current: dict) -> bool:
    p = str(current.get("attachment_path") or "").strip()
    return bool(p) and Path(p).exists()


def _default_purchase_invoice_number() -> str:
    y = date.today().year
    prefix = f"R{y}-"
    with engine.begin() as conn:
        r = conn.execute(text("""
            SELECT supplier_invoice_number
            FROM purchase_invoices
            WHERE supplier_invoice_number LIKE :prefix
            ORDER BY id DESC
            LIMIT 1
        """), {"prefix": f"{prefix}%"}).mappings().first()
    if not r or not r.get("supplier_invoice_number"):
        return f"{prefix}0001"
    m = re.search(r"(\d+)$", str(r["supplier_invoice_number"]))
    nxt = int(m.group(1)) + 1 if m else 1
    return f"{prefix}{nxt:04d}"


def render_purchase_invoices_admin(actor: str):
    st.subheader("Administrador · Facturas recibidas")
    st.caption("Alta en base propia + cola de sincronización a myGestión.")

    sdf = supplier_options()
    if sdf.empty:
        st.warning("Primero debes crear al menos un proveedor activo.")
        return

    supplier_labels = {f"{int(r['id'])} · {r['name']}": int(r['id']) for _, r in sdf.iterrows()}
    supplier_label_list = list(supplier_labels.keys())

    tab1, tab2, tab3 = st.tabs(["Alta manual", "Alta desde PDF", "Editar / consultar"])

    with tab1:
        with st.form("form_add_purchase_invoice", clear_on_submit=False):
            c1, c2, c3 = st.columns(3)
            supplier_invoice_number = c1.text_input("Número factura proveedor", value=_default_purchase_invoice_number())
            invoice_date = c2.date_input("Fecha factura", value=date.today(), key="purchase_invoice_date_new")
            supplier_label = c3.selectbox("Proveedor", options=supplier_label_list, index=0, key="purchase_invoice_supplier_new")

            c4, c5, c6, c7 = st.columns(4)
            base_amount = c4.number_input("Base imponible (€)", min_value=0.0, step=10.0, value=0.0, key="purchase_base_new")
            tax_rate = c5.number_input("IVA (%)", min_value=0.0, max_value=100.0, step=1.0, value=21.0, key="purchase_tax_new")
            status = c6.selectbox("Estado", options=["draft", "received", "paid", "cancelled"], index=0, key="purchase_invoice_status_new")
            deductible = c7.checkbox("Deducible", value=True, key="purchase_invoice_deductible_new")

            preview_tax = round(float(base_amount) * float(tax_rate) / 100.0, 2)
            preview_total = round(float(base_amount) + preview_tax, 2)
            st.caption(f"IVA calculado: {preview_tax:.2f} € · Total: {preview_total:.2f} €")
            notes = st.text_area("Notas", height=90, key="purchase_invoice_notes_new")
            uploaded_file = st.file_uploader(
                "Adjuntar PDF o imagen",
                type=["pdf", "png", "jpg", "jpeg", "webp"],
                key="purchase_invoice_attachment_new",
                help="Opcional. Se guardará junto a la factura recibida en la base documental interna.",
            )

            if st.form_submit_button("Guardar factura recibida"):
                try:
                    supplier_id = supplier_labels[supplier_label]
                    new_id = add_purchase_invoice(actor, supplier_invoice_number, invoice_date.isoformat(), supplier_id, base_amount, tax_rate, deductible, notes, status)
                    if uploaded_file is not None:
                        save_purchase_invoice_attachment(new_id, uploaded_file, actor)
                    st.success(f"Factura recibida creada (ID {new_id}).")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

    with tab2:
        st.markdown("**Cargar PDF y rellenar automáticamente**")
        st.caption("Funciona mejor con PDFs que ya contienen texto. Si el PDF es una imagen escaneada, puede requerir revisión manual.")
        uploaded_pdf = st.file_uploader(
            "Factura recibida en PDF",
            type=["pdf"],
            key="purchase_invoice_pdf_autofill",
        )
        cpdf1, cpdf2 = st.columns([1, 2])
        if cpdf1.button("Leer PDF y preparar borrador", key="purchase_invoice_pdf_parse"):
            if uploaded_pdf is None:
                st.warning("Selecciona un PDF antes de analizar.")
            else:
                parsed = parse_purchase_invoice_pdf(uploaded_pdf)
                st.session_state["purchase_pdf_draft"] = parsed
                st.session_state["purchase_pdf_attachment_bytes"] = uploaded_pdf.getvalue()
                st.session_state["purchase_pdf_attachment_name"] = getattr(uploaded_pdf, "name", "factura.pdf")
                st.session_state["purchase_pdf_attachment_type"] = getattr(uploaded_pdf, "type", "application/pdf") or "application/pdf"
                if parsed.get("ok"):
                    st.success("PDF leído. Revisa los datos propuestos antes de guardar.")
                else:
                    st.warning("No se pudo leer el PDF con suficiente calidad. Puedes completar los campos manualmente.")

        draft = st.session_state.get("purchase_pdf_draft")
        if draft:
            for msg in draft.get("warnings", []):
                st.warning(msg)

            default_supplier_label = draft.get("supplier_match_label") if draft.get("supplier_match_label") in supplier_label_list else supplier_label_list[0]
            default_supplier_index = supplier_label_list.index(default_supplier_label)
            default_status = "received"
            status_options = ["draft", "received", "paid", "cancelled"]

            with st.form("form_add_purchase_invoice_from_pdf", clear_on_submit=False):
                p1, p2, p3 = st.columns(3)
                supplier_invoice_number = p1.text_input("Número factura proveedor", value=str(draft.get("supplier_invoice_number") or _default_purchase_invoice_number()))
                invoice_date = p2.date_input("Fecha factura", value=pd.to_datetime(draft.get("invoice_date") or date.today().isoformat()).date(), key="purchase_pdf_invoice_date")
                supplier_label = p3.selectbox("Proveedor", options=supplier_label_list, index=default_supplier_index, key="purchase_pdf_supplier")

                p4, p5, p6, p7 = st.columns(4)
                base_amount = p4.number_input("Base imponible (€)", min_value=0.0, step=10.0, value=float(draft.get("base_amount") or 0.0), key="purchase_pdf_base")
                tax_rate = p5.number_input("IVA (%)", min_value=0.0, max_value=100.0, step=1.0, value=float(draft.get("tax_rate") or 21.0), key="purchase_pdf_tax")
                status = p6.selectbox("Estado", options=status_options, index=status_options.index(default_status), key="purchase_pdf_status")
                deductible = p7.checkbox("Deducible", value=True, key="purchase_pdf_deductible")

                preview_tax = round(float(base_amount) * float(tax_rate) / 100.0, 2)
                preview_total = round(float(base_amount) + preview_tax, 2)
                st.caption(f"IVA calculado: {preview_tax:.2f} € · Total: {preview_total:.2f} €")
                notes_default = str(draft.get("notes") or "")
                notes = st.text_area("Notas", value=notes_default, height=90, key="purchase_pdf_notes")

                save_pdf_form = st.form_submit_button("Guardar factura recibida desde PDF")
                if save_pdf_form:
                    try:
                        supplier_id = supplier_labels[supplier_label]
                        new_id = add_purchase_invoice(actor, supplier_invoice_number, invoice_date.isoformat(), supplier_id, base_amount, tax_rate, deductible, notes, status)
                        pdf_bytes = st.session_state.get("purchase_pdf_attachment_bytes")
                        if pdf_bytes:
                            class _MemoryUpload:
                                def __init__(self, data, name, mime):
                                    self._data = data
                                    self.name = name
                                    self.type = mime
                                def getbuffer(self):
                                    return self._data
                            save_purchase_invoice_attachment(
                                new_id,
                                _MemoryUpload(
                                    pdf_bytes,
                                    st.session_state.get("purchase_pdf_attachment_name", "factura.pdf"),
                                    st.session_state.get("purchase_pdf_attachment_type", "application/pdf"),
                                ),
                                actor,
                            )
                        st.success(f"Factura recibida creada desde PDF (ID {new_id}).")
                        for k in ["purchase_pdf_draft", "purchase_pdf_attachment_bytes", "purchase_pdf_attachment_name", "purchase_pdf_attachment_type"]:
                            st.session_state.pop(k, None)
                        st.rerun()
                    except Exception as e:
                        st.error(str(e))
        else:
            st.info("Sube un PDF y pulsa 'Leer PDF y preparar borrador' para autocompletar la factura.")

    with tab3:
        df = purchase_invoices_df(limit=500)
        if df.empty:
            st.info("Todavía no hay facturas recibidas.")
            return
        df_view = df.copy()
        df_view["Adjunto"] = df_view["attachment_name"].fillna("").apply(lambda x: "Sí" if str(x).strip() else "No")
        st.dataframe(df_view, use_container_width=True, hide_index=True)
        selected_id = st.selectbox("Factura recibida", options=df["id"].astype(int).tolist(), key="purchase_invoice_edit_id")
        current = get_purchase_invoice(int(selected_id))
        if not current:
            st.warning("No se pudo cargar la factura seleccionada.")
            return

        default_supplier_label = next((lbl for lbl, sid in supplier_labels.items() if sid == int(current["supplier_id"])), supplier_label_list[0])
        default_supplier_index = supplier_label_list.index(default_supplier_label)

        st.markdown("**Documento adjunto**")
        if attachment_exists(current):
            attachment_path = Path(str(current.get("attachment_path")))
            st.success(f"Adjunto actual: {attachment_download_name(current)}")
            with open(attachment_path, "rb") as f:
                st.download_button(
                    "Descargar adjunto",
                    data=f.read(),
                    file_name=attachment_download_name(current),
                    mime=str(current.get("attachment_mime") or "application/octet-stream"),
                    key=f"download_purchase_attachment_{selected_id}",
                )
        else:
            st.info("Esta factura todavía no tiene adjunto.")

        replacement_file = st.file_uploader(
            "Subir o reemplazar adjunto",
            type=["pdf", "png", "jpg", "jpeg", "webp"],
            key=f"purchase_attachment_replace_{selected_id}",
            help="Puedes sustituir el justificante actual por uno nuevo.",
        )
        if st.button("Guardar adjunto", key=f"save_purchase_attachment_{selected_id}"):
            if replacement_file is None:
                st.warning("Selecciona un archivo antes de guardar.")
            else:
                try:
                    save_purchase_invoice_attachment(int(selected_id), replacement_file, actor)
                    st.success("Adjunto guardado correctamente.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))

        with st.form(f"form_edit_purchase_invoice_{selected_id}"):
            e1, e2, e3 = st.columns(3)
            supplier_invoice_number = e1.text_input("Número factura proveedor", value=str(current["supplier_invoice_number"] or ""))
            invoice_date = e2.date_input("Fecha factura", value=pd.to_datetime(current["invoice_date"]).date(), key=f"purchase_date_{selected_id}")
            supplier_label = e3.selectbox("Proveedor", options=supplier_label_list, index=default_supplier_index, key=f"purchase_supplier_{selected_id}")

            e4, e5, e6, e7 = st.columns(4)
            base_amount = e4.number_input("Base imponible (€)", min_value=0.0, step=10.0, value=float(current["base_amount"]), key=f"purchase_base_{selected_id}")
            tax_rate = e5.number_input("IVA (%)", min_value=0.0, max_value=100.0, step=1.0, value=float(current.get("tax_rate") or 21.0), key=f"purchase_tax_{selected_id}")
            status = e6.selectbox("Estado", options=["draft", "received", "paid", "cancelled"], index=["draft", "received", "paid", "cancelled"].index(str(current.get("status") or "draft")), key=f"purchase_status_{selected_id}")
            deductible = e7.checkbox("Deducible", value=bool(int(current.get("deductible") or 0)), key=f"purchase_deductible_{selected_id}")

            preview_tax = round(float(base_amount) * float(tax_rate) / 100.0, 2)
            preview_total = round(float(base_amount) + preview_tax, 2)
            st.caption(f"IVA calculado: {preview_tax:.2f} € · Total: {preview_total:.2f} €")
            notes = st.text_area("Notas", value=str(current.get("notes") or ""), height=90, key=f"purchase_notes_{selected_id}")

            if st.form_submit_button("Guardar cambios"):
                try:
                    supplier_id = supplier_labels[supplier_label]
                    update_purchase_invoice(int(selected_id), actor, supplier_invoice_number, invoice_date.isoformat(), supplier_id, base_amount, tax_rate, deductible, notes, status)
                    st.success("Factura recibida actualizada.")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))





def render_purchase_documents_center(actor: str):
    st.subheader("Administrador · Justificantes de gasto")
    st.caption("Consulta, descarga y reemplazo de PDFs e imágenes asociados a facturas recibidas.")

    df = purchase_invoices_df(limit=5000)
    if df.empty:
        st.info("Todavía no hay facturas recibidas.")
        return

    work = df.copy()
    work["attachment_flag"] = work["attachment_name"].fillna("").astype(str).str.strip().ne("")
    work["Estado"] = work["status"].map(INVOICE_STATUS_LABELS).fillna(work["status"])
    proveedores = ["Todos"] + sorted(work["supplier_name"].fillna("").astype(str).replace("", "(Sin proveedor)").unique().tolist())

    c1, c2, c3 = st.columns(3)
    supplier_filter = c1.selectbox("Proveedor", options=proveedores, index=0, key="docs_supplier_filter")
    attachment_filter = c2.selectbox("Adjunto", options=["Todos", "Con adjunto", "Sin adjunto"], index=0, key="docs_attachment_filter")
    status_filter = c3.selectbox(
        "Estado",
        options=["Todos", "Borrador", "Recibida", "Pagada", "Cancelada"],
        index=0,
        key="docs_status_filter",
    )

    if supplier_filter != "Todos":
        work = work[work["supplier_name"].fillna("").replace("", "(Sin proveedor)") == supplier_filter]
    if attachment_filter == "Con adjunto":
        work = work[work["attachment_flag"]]
    elif attachment_filter == "Sin adjunto":
        work = work[~work["attachment_flag"]]
    if status_filter != "Todos":
        reverse_status = {v: k for k, v in INVOICE_STATUS_LABELS.items()}
        work = work[work["status"] == reverse_status.get(status_filter, status_filter)]

    work = work.sort_values(["invoice_date", "id"], ascending=[False, False]).reset_index(drop=True)

    resumen = work.copy()
    resumen["Adjunto"] = resumen["attachment_flag"].map({True: "Sí", False: "No"})
    resumen = resumen[["id", "supplier_name", "supplier_invoice_number", "invoice_date", "Estado", "total_amount", "Adjunto", "attachment_name"]]
    resumen = resumen.rename(columns={
        "id": "ID",
        "supplier_name": "Proveedor",
        "supplier_invoice_number": "Factura proveedor",
        "invoice_date": "Fecha",
        "total_amount": "Total (€)",
        "attachment_name": "Nombre adjunto",
    })
    st.dataframe(resumen, use_container_width=True, hide_index=True)

    if work.empty:
        st.info("No hay resultados con los filtros seleccionados.")
        return

    selected_id = st.selectbox("Factura para revisar", options=work["id"].astype(int).tolist(), key="docs_selected_invoice")
    current = get_purchase_invoice(int(selected_id))
    if not current:
        st.warning("No se pudo cargar la factura seleccionada.")
        return

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Proveedor", str(current.get("supplier_name") or "-"))
    m2.metric("Factura", str(current.get("supplier_invoice_number") or "-"))
    m3.metric("Fecha", str(current.get("invoice_date") or "-"))
    m4.metric("Total", f"{float(current.get('total_amount') or 0):,.2f} €")

    st.markdown("**Detalle del adjunto**")
    if attachment_exists(current):
        attachment_path = Path(str(current.get("attachment_path")))
        mime = str(current.get("attachment_mime") or "application/octet-stream")
        st.success(f"Archivo: {attachment_download_name(current)}")
        meta1, meta2, meta3 = st.columns(3)
        meta1.write(f"**Tipo:** {mime}")
        meta2.write(f"**Subido por:** {current.get('attachment_uploaded_by') or '-'}")
        meta3.write(f"**Fecha subida:** {current.get('attachment_uploaded_at') or '-'}")
        with open(attachment_path, 'rb') as f:
            payload = f.read()
        st.download_button(
            "Descargar adjunto",
            data=payload,
            file_name=attachment_download_name(current),
            mime=mime,
            key=f"docs_download_{selected_id}",
        )
        if mime.startswith("image/"):
            st.image(payload, caption=attachment_download_name(current), use_container_width=True)
        elif mime == "application/pdf":
            st.info("Vista previa PDF no embebida. Puedes descargar el archivo para revisarlo.")
    else:
        st.warning("Esta factura no tiene adjunto todavía.")

    replacement_file = st.file_uploader(
        "Subir o reemplazar adjunto desde el centro documental",
        type=["pdf", "png", "jpg", "jpeg", "webp"],
        key=f"docs_replace_file_{selected_id}",
    )
    if st.button("Guardar adjunto en esta factura", key=f"docs_save_attachment_{selected_id}"):
        if replacement_file is None:
            st.warning("Selecciona un archivo antes de guardar.")
        else:
            try:
                save_purchase_invoice_attachment(int(selected_id), replacement_file, actor)
                st.success("Adjunto guardado correctamente.")
                st.rerun()
            except Exception as e:
                st.error(str(e))


def render_accounting_dashboard():
    st.subheader("Dashboard · Contabilidad y sincronización")
    st.caption("Resumen operativo de clientes, proveedores, facturas y cola de sincronización.")

    sdf = sales_invoices_df(limit=5000)
    pdf = purchase_invoices_df(limit=5000)
    qdf = sync_queue_df(status="all", limit=1000)

    emitted_count = 0 if sdf.empty else int(len(sdf))
    received_count = 0 if pdf.empty else int(len(pdf))
    emitted_total = 0.0 if sdf.empty else float(pd.to_numeric(sdf["total_amount"], errors="coerce").fillna(0).sum())
    received_total = 0.0 if pdf.empty else float(pd.to_numeric(pdf["total_amount"], errors="coerce").fillna(0).sum())
    pending_sync = 0 if qdf.empty else int((qdf["status"] == "pending").sum())
    sync_errors = 0 if qdf.empty else int((qdf["status"] == "error").sum())

    attached_received = 0 if pdf.empty else int(pdf["attachment_name"].fillna("").astype(str).str.strip().ne("").sum())

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Facturas emitidas", emitted_count, f"{emitted_total:,.2f} €")
    m2.metric("Facturas recibidas", received_count, f"{received_total:,.2f} €")
    m3.metric("Con adjunto", attached_received)
    m4.metric("Pendientes de sincronizar", pending_sync)
    m5.metric("Errores de sincronización", sync_errors)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Facturación emitida por estado**")
        if sdf.empty:
            st.info("Aún no hay facturas emitidas.")
        else:
            tmp = sdf.copy()
            tmp["Estado"] = tmp["status"].map(INVOICE_STATUS_LABELS).fillna(tmp["status"])
            resumen = tmp.groupby("Estado", dropna=False)[["base_amount", "tax_amount", "total_amount"]].sum().reset_index()
            resumen = resumen.rename(columns={
                "base_amount": "Base imponible (€)",
                "tax_amount": "IVA (€)",
                "total_amount": "Total (€)",
            })
            st.dataframe(resumen, use_container_width=True, hide_index=True)

    with c2:
        st.markdown("**Facturación recibida por estado**")
        if pdf.empty:
            st.info("Aún no hay facturas recibidas.")
        else:
            tmp = pdf.copy()
            tmp["Estado"] = tmp["status"].map(INVOICE_STATUS_LABELS).fillna(tmp["status"])
            resumen = tmp.groupby("Estado", dropna=False)[["base_amount", "tax_amount", "total_amount"]].sum().reset_index()
            resumen = resumen.rename(columns={
                "base_amount": "Base imponible (€)",
                "tax_amount": "IVA (€)",
                "total_amount": "Total (€)",
            })
            st.dataframe(resumen, use_container_width=True, hide_index=True)

    st.markdown("**Facturas recibidas y documentación**")
    if pdf.empty:
        st.info("Aún no hay facturas recibidas.")
    else:
        docs = pdf.copy()
        docs["Estado"] = docs["status"].map(INVOICE_STATUS_LABELS).fillna(docs["status"])
        docs["Adjunto"] = docs["attachment_name"].fillna("").apply(lambda x: "Sí" if str(x).strip() else "No")
        docs = docs[["id", "supplier_name", "supplier_invoice_number", "invoice_date", "Estado", "total_amount", "Adjunto", "attachment_name"]]
        docs = docs.rename(columns={
            "id": "ID",
            "supplier_name": "Proveedor",
            "supplier_invoice_number": "Factura proveedor",
            "invoice_date": "Fecha",
            "total_amount": "Total (€)",
            "attachment_name": "Nombre adjunto",
        })
        st.dataframe(docs.head(20), use_container_width=True, hide_index=True)

    st.markdown("**Cola de sincronización**")
    if qdf.empty:
        st.info("No hay elementos en la cola.")
    else:
        q = qdf.copy()
        q["Entidad"] = q["entity_type"].map(ENTITY_TYPE_LABELS).fillna(q["entity_type"])
        q["Acción"] = q["action"].map(ACTION_LABELS).fillna(q["action"])
        q["Estado"] = q["status"].map(SYNC_STATUS_LABELS).fillna(q["status"])
        status_summary = q.groupby(["Entidad", "Estado"], dropna=False).size().reset_index(name="Registros")
        st.dataframe(status_summary, use_container_width=True, hide_index=True)

        st.markdown("**Últimos movimientos**")
        recent = q[["id", "Entidad", "Acción", "Estado", "attempts", "created_at", "processed_at", "last_error"]].head(15)
        recent = recent.rename(columns={
            "id": "ID",
            "attempts": "Intentos",
            "created_at": "Creado",
            "processed_at": "Procesado",
            "last_error": "Último error",
        })
        st.dataframe(recent, use_container_width=True, hide_index=True)

def render_sync_admin():
    st.subheader("Administrador · Sincronización myGestión")
    st.caption("Cola interna para altas y cambios pendientes de enviar al ERP.")

    cfg = mygestion_settings()
    with st.expander("Configuración myGestión", expanded=False):
        st.write({
            "base_url": cfg.get("base_url") or "",
            "username": cfg.get("username") or "",
            "endpoints": cfg.get("endpoints") or {},
            "enabled": cfg.get("enabled") or False,
        })
        b1, b2 = st.columns(2)
        if b1.button("Probar conexión", key="myg_test_connection"):
            ok, msg = mygestion_test_connection()
            (st.success if ok else st.error)(msg)
        if b2.button("Sincronizar pendientes", key="myg_sync_pending"):
            ok_count, err_count, messages = process_sync_batch(limit=20)
            if ok_count:
                st.success(f"Sincronizados: {ok_count}")
            if err_count:
                st.error(f"Con error: {err_count}")
                for msg in messages[:10]:
                    st.write(f"- {msg}")
            st.rerun()

    c1, c2 = st.columns([1, 3])
    status_filter = c1.selectbox("Estado", options=["all", "pending", "processing", "done", "error"], index=0, key="sync_status_filter", format_func=lambda x: "Todos" if x == "all" else SYNC_STATUS_LABELS.get(x, x))
    limit = c2.slider("Máximo de filas", min_value=20, max_value=500, value=100, step=20, key="sync_limit")

    df = sync_queue_df(status=status_filter, limit=limit)
    if df.empty:
        st.info("La cola de sincronización está vacía.")
        return

    df_view = df.copy()
    df_view["Entidad"] = df_view["entity_type"].map(ENTITY_TYPE_LABELS).fillna(df_view["entity_type"])
    df_view["Acción"] = df_view["action"].map(ACTION_LABELS).fillna(df_view["action"])
    df_view["Estado"] = df_view["status"].map(SYNC_STATUS_LABELS).fillna(df_view["status"])
    df_view = df_view.rename(columns={
        "id": "ID",
        "attempts": "Intentos",
        "created_at": "Creado",
        "processed_at": "Procesado",
        "last_error": "Último error",
        "entity_id": "ID entidad",
    })
    show_cols = [c for c in ["ID", "Entidad", "ID entidad", "Acción", "Estado", "Intentos", "Creado", "Procesado", "Último error"] if c in df_view.columns]
    st.dataframe(df_view[show_cols], use_container_width=True, hide_index=True)

    selected_id = st.selectbox("Registro de cola", options=df["id"].astype(int).tolist(), index=0, key="sync_selected_id")
    current = df[df["id"] == selected_id].iloc[0].to_dict()
    payload_raw = current.get("payload_json") or "{}"

    c3, c4, c5 = st.columns([1, 1, 1])
    if c3.button("Procesar ahora", key=f"process_{selected_id}"):
        ok, msg = process_sync_item(int(selected_id))
        (st.success if ok else st.error)(msg)
        st.rerun()
    if c4.button("Marcar como hecho", key=f"done_{selected_id}"):
        update_sync_status(int(selected_id), "done", "")
        st.rerun()
    if c5.button("Reencolar", key=f"retry_{selected_id}"):
        reset_sync_item(int(selected_id))
        st.rerun()

    error_msg = st.text_input("Error / nota de seguimiento", value=str(current.get("last_error") or ""), key=f"err_{selected_id}")
    if st.button("Marcar como error", key=f"mark_error_{selected_id}"):
        update_sync_status(int(selected_id), "error", error_msg)
        st.rerun()

    st.code(payload_raw, language="json")


# =======================
# UI SESSION
# =======================
if "auth" not in st.session_state:
    st.session_state["auth"] = False
    st.session_state["role"] = None
    st.session_state["user"] = None


# =======================
# LOGIN (desplegable)
# =======================
if not st.session_state["auth"]:
    with st.form("login_form"):
        st.subheader("Acceso (PIN)")

        users = list_users_for_login()
        if not users:
            st.error("No hay usuarios en la base de datos.")
            st.stop()

        u = st.selectbox("Usuario", options=users, index=0)
        p = st.text_input("PIN", type="password")

        if st.form_submit_button("Entrar"):
            ok, role = login(u, p)
            if ok:
                st.session_state["auth"] = True
                st.session_state["role"] = role
                st.session_state["user"] = u
                st.rerun()
            else:
                st.error("PIN incorrecto.")
    st.info("Usuarios por defecto: owner/1234, comercial1/1111, comercial2/2222, empleado1/0000")
    st.stop()


# =======================
# NAV
# =======================
role = st.session_state["role"]
user = st.session_state["user"]

st.sidebar.write(f"👤 {user} | Rol: **{ROLE_LABELS.get(role, role)}**")

if role == ROLE_OWNER:
    page = st.sidebar.radio("Pantallas", [
        "Dashboard · Dirección",
        "Administrador · Dashboard contable",
        "Administrador · Clientes",
        "Administrador · Proveedores",
        "Administrador · Facturas emitidas",
        "Administrador · Facturas recibidas",
        "Administrador · Justificantes de gasto",
        "Administrador · Sincronización myGestión",
        "Administrador · Bandeja de PDFs",
        "Administrador · Modelos y precios",
        "Administrador · Previsión ventas firmadas",
        "Administrador · Previsión gastos",
        "Comercial · Ventas firmadas (modo administrador)",
        "Empleado/a · Subir PDF (modo administrador)",
        "Administrador · Usuarios",
        "Salir",
    ])
elif role == ROLE_EMPLOYEE:
    page = st.sidebar.radio("Pantallas", [
        "Empleado/a · Subir PDF",
        "Salir",
    ])
else:
    page = st.sidebar.radio("Pantallas", [
        "Comercial · Ventas firmadas",
        "Salir",
    ])

if page == "Salir":
    st.session_state["auth"] = False
    st.session_state["role"] = None
    st.session_state["user"] = None
    st.rerun()


# =======================
# ADMIN · CLIENTES / PROVEEDORES / SYNC
# =======================
if page == "Administrador · Clientes":
    require_role([ROLE_OWNER])
    render_customers_admin(user)

if page == "Administrador · Proveedores":
    require_role([ROLE_OWNER])
    render_suppliers_admin(user)

if page == "Administrador · Facturas emitidas":
    require_role([ROLE_OWNER])
    render_sales_invoices_admin(user)

if page == "Administrador · Facturas recibidas":
    require_role([ROLE_OWNER])
    render_purchase_invoices_admin(user)

if page == "Administrador · Justificantes de gasto":
    require_role([ROLE_OWNER])
    render_purchase_documents_center(user)

if page == "Administrador · Sincronización myGestión":
    require_role([ROLE_OWNER])
    render_sync_admin()


if page == "Administrador · Dashboard contable":
    require_role([ROLE_OWNER])
    render_accounting_dashboard()


# =======================
# EMPLEADO/A: SUBIR PDF
# =======================
if page in ("Empleado/a · Subir PDF", "Empleado/a · Subir PDF (modo administrador)"):
    require_role([ROLE_EMPLOYEE, ROLE_OWNER])
    st.subheader("Subir PDF (entrada de datos)")
    st.caption("Histórico: ANUAL. Año en curso: por defecto usa YYYY-MM del mes actual (si más adelante lo usas mensual).")

    year = st.selectbox("Ejercicio (año)", options=[CURRENT_YEAR] + HIST_YEARS, index=0)
    scope = "CURRENT" if int(year) == CURRENT_YEAR else "HISTORICAL"

    if year_is_closed(int(year)) and role != ROLE_OWNER:
        st.warning(f"⚠️ El ejercicio {year} está CERRADO.")
        st.stop()

    period = "ANUAL" if scope == "HISTORICAL" else f"{int(year)}-{MONTHS[date.today().month - 1]}"

    with st.form("upload_pdf", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        doc_type = c1.selectbox("Tipo de documento", DOC_TYPES)
        bank = c2.text_input("Banco (solo si aplica)", value="")
        _ = c3.text_input("Periodo", value=period, disabled=True)

        pdf = st.file_uploader("Selecciona PDF", type=["pdf"])
        submitted = st.form_submit_button("Subir y procesar")

        if submitted:
            if not pdf:
                st.error("Sube un PDF.")
                st.stop()

            bank_norm = (bank or "").strip()

            if doc_exists(scope, doc_type, int(year), period, bank_norm):
                st.error("Ya existe un documento con misma combinación (scope/tipo/año/periodo/banco).")
                st.stop()

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = re.sub(r"[^a-zA-Z0-9_\-\.]", "_", pdf.name)
            out = PDF_DIR / f"{ts}_Y{year}_{period}_{doc_type}_{safe_name}"
            with open(out, "wb") as f:
                f.write(pdf.getbuffer())

            doc_id = insert_document(
                uploaded_by=user,
                doc_type=doc_type,
                year=int(year),
                scope=scope,
                period=period,
                bank=bank_norm,
                filepath=out,
                status="uploaded",
                parse_log="",
            )

            rows, log = spot_document(doc_type, str(out))
            clear_extracted_rows(doc_id)
            insert_extracted_rows(doc_id, rows)

            new_status = "pending_validation" if rows else "error"
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE documents SET status=:s, parse_log=:l WHERE id=:id"),
                    {"s": new_status, "l": log, "id": int(doc_id)},
                )

            if rows:
                st.success(f"✅ PDF subido. Detectadas {len(rows)} filas. Pendiente de validación.")
            else:
                st.warning(f"PDF subido pero sin extracción útil. Motivo: {log}")


# =======================
# COMERCIAL: VENTAS FIRMADAS
# - Comercial: SOLO sus ventas; created_by fijo = usuario logueado
# - Admin (modo administrador): puede elegir comercial con desplegable
# =======================
def render_sales_entry(title: str, is_admin_mode: bool):
    st.subheader(title)
    st.caption("Modelo y tarifa salen de la base de datos. El precio final es editable.")

    year = CURRENT_YEAR
    month = st.selectbox("Mes previsto", options=MONTHS, index=date.today().month - 1)
    period = f"{year}-{month}"

    # ---- asignación de comercial ----
    if is_admin_mode:
        comm_users = list_commercial_users()
        if not comm_users:
            st.error("No hay usuarios con rol Comercial. Crea alguno en 'Administrador · Usuarios'.")
            st.stop()
        created_by = st.selectbox("Asignar venta al comercial", options=comm_users, index=0)
    else:
        created_by = user  # el comercial logueado

    mdf = models_df(active_only=True)
    if mdf.empty:
        st.error("No hay modelos activos. Crea modelos en 'Administrador · Modelos y precios'.")
        st.stop()

    mdf = mdf.copy()
    mdf["label"] = mdf.apply(
        lambda r: f"{r['code']} — {r['name']} ({r['fuel']}) | Tarifa {float(r['list_price']):.2f}€",
        axis=1
    )
    selected_label = st.selectbox("Modelo", options=mdf["label"].tolist())
    selected_row = mdf[mdf["label"] == selected_label].iloc[0]
    model_id = int(selected_row["id"])
    default_price = float(selected_row["list_price"])

    with st.form("signed_sale_form", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        customer = c1.text_input("Cliente / Obra")
        units = c2.number_input("Unidades", min_value=1, step=1, value=1)
        unit_price = c3.number_input("Precio unitario (€)", min_value=0.0, step=10.0, value=float(default_price))
        notes = st.text_input("Notas (opcional)")

        if st.form_submit_button("Guardar venta firmada"):
            if not customer.strip():
                st.error("Cliente/obra es obligatorio.")
            else:
                add_signed_sale(
                    created_by=created_by,
                    year=year,
                    period=period,
                    customer=customer,
                    model_id=model_id,
                    units=int(units),
                    unit_price=float(unit_price),
                    notes=notes,
                )
                st.success("✅ Venta firmada registrada.")
                st.rerun()

    st.divider()
    if is_admin_mode:
        st.subheader("Ventas firmadas (todas, año en curso)")
        df = signed_sales_df(CURRENT_YEAR, ROLE_OWNER, user)
    else:
        st.subheader("Mis ventas firmadas (año en curso)")
        df = signed_sales_df(CURRENT_YEAR, ROLE_COMMERCIAL, user)

    st.dataframe(df, use_container_width=True)


if page == "Comercial · Ventas firmadas":
    require_role([ROLE_COMMERCIAL])
    render_sales_entry("Comercial · Ventas firmadas", is_admin_mode=False)

if page == "Comercial · Ventas firmadas (modo administrador)":
    require_role([ROLE_OWNER])
    render_sales_entry("Comercial · Ventas firmadas (modo administrador)", is_admin_mode=True)


# =======================
# ADMIN: PREVISIÓN
# =======================
if page == "Administrador · Previsión ventas firmadas":
    require_role([ROLE_OWNER])
    st.subheader("Previsión — Ventas firmadas (año en curso)")

    df_all, monthly, by_salesperson = signed_sales_monthly_summary(CURRENT_YEAR)

    if df_all.empty:
        st.info("No hay ventas firmadas registradas aún.")
        st.stop()

    total_prev = float(df_all["total"].sum())
    st.metric("Total previsto (ventas firmadas)", f"{total_prev:,.2f} €".replace(",", "X").replace(".", ",").replace("X", "."))
    st.metric("Operaciones", f"{len(df_all)}")

    st.divider()
    st.subheader("Resumen mensual (€)")

    # --- Gastos previstos (presupuesto) ---
    exp_monthly = expense_forecast_monthly_summary(CURRENT_YEAR)
    monthly_full = pd.DataFrame({"period": [f"{CURRENT_YEAR}-{m}" for m in MONTHS]})
    monthly_full = monthly_full.merge(monthly, on="period", how="left").fillna({"total": 0.0})
    monthly_full = monthly_full.merge(exp_monthly, on="period", how="left").fillna({"expenses": 0.0})
    monthly_full["resultado"] = (monthly_full["total"].astype(float) - monthly_full["expenses"].astype(float)).round(2)
    monthly_full["acumulado"] = monthly_full["resultado"].cumsum().round(2)

    c1, c2, c3 = st.columns(3)
    total_exp = float(monthly_full["expenses"].sum())
    total_res = float(monthly_full["resultado"].sum())
    c1.metric("Gastos previstos (presupuesto)", f"{total_exp:,.2f} €".replace(",", "X").replace(".", ",").replace("X", "."))
    c2.metric("Resultado previsto (ventas - gastos)", f"{total_res:,.2f} €".replace(",", "X").replace(".", ",").replace("X", "."))
    c3.metric("Acumulado a fin de año", f"{float(monthly_full['acumulado'].iloc[-1]):,.2f} €".replace(",", "X").replace(".", ",").replace("X", "."))

    st.dataframe(monthly_full.rename(columns={"total":"ventas_firmadas","expenses":"gastos_previstos"}), use_container_width=True)

    st.subheader("Por comercial (€)")
    st.dataframe(by_salesperson, use_container_width=True)

    st.subheader("Detalle (todas)")
    st.dataframe(df_all, use_container_width=True)


# =======================
# ADMIN: PREVISIÓN GASTOS
# =======================
if page == "Administrador · Previsión gastos":
    require_role([ROLE_OWNER])
    st.subheader("Previsión de gastos (presupuesto)")

    year = st.selectbox("Ejercicio (año)", options=[CURRENT_YEAR] + HIST_YEARS, index=0, key="exp_year")
    st.caption("Esto es **presupuesto** para afinar la previsión de beneficio. No sustituye la contabilidad oficial.")

    with st.form("exp_add", clear_on_submit=True):
        c1, c2, c3, c4 = st.columns([1.2, 1.8, 1.5, 1.5])
        month = c1.selectbox("Mes", options=MONTHS, index=date.today().month - 1)
        period = f"{int(year)}-{month}"
        category = c2.selectbox("Categoría", options=DEFAULT_EXPENSE_CATEGORIES)
        amount = c3.number_input("Importe (€) (gasto)", min_value=0.0, step=50.0, value=0.0)
        notes = c4.text_input("Notas", value="")
        submitted = st.form_submit_button("Guardar / Actualizar")

        if submitted:
            if amount <= 0:
                st.error("El importe debe ser mayor que 0.")
            else:
                upsert_expense_forecast(int(year), period, category, float(amount), notes, actor=user)
                st.success("✅ Presupuesto guardado.")
                st.rerun()

    st.divider()

    df = expense_forecast_df(int(year))
    if df.empty:
        st.info("Aún no hay presupuesto de gastos para este año.")
    else:
        st.subheader("Detalle presupuesto")
        st.dataframe(
            df[["id", "period", "category", "amount", "notes", "created_by", "created_at", "updated_by", "updated_at"]],
            use_container_width=True
        )

        st.subheader("Resumen mensual")
        m = expense_forecast_monthly_summary(int(year))
        monthly_full = pd.DataFrame({"period": [f"{int(year)}-{mm}" for mm in MONTHS]})
        monthly_full = monthly_full.merge(m, on="period", how="left").fillna({"expenses": 0.0})
        monthly_full["acumulado"] = monthly_full["expenses"].cumsum().round(2)

        st.metric(
            "Total gastos previstos",
            f"{float(monthly_full['expenses'].sum()):,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")
        )
        st.dataframe(monthly_full.rename(columns={"expenses": "gastos_previstos"}), use_container_width=True)

        st.divider()
        st.subheader("Eliminar línea")
        ids = df["id"].astype(int).tolist()
        del_id = st.selectbox("ID a eliminar", options=ids)
        confirm = st.checkbox("Confirmo que quiero eliminar esta línea (irreversible)", key="exp_del_confirm")
        if st.button("🗑️ Eliminar", key="exp_del_btn"):
            if not confirm:
                st.error("Marca la confirmación para eliminar.")
            else:
                delete_expense_forecast(int(del_id))
                st.success("Línea eliminada.")
                st.rerun()

    st.divider()
    st.subheader("Copiar presupuesto de un año a otro")
    c1, c2, c3 = st.columns(3)
    from_y = c1.selectbox("Desde", options=HIST_YEARS + [CURRENT_YEAR], index=0, key="copy_from_y")
    to_y = c2.selectbox("Hacia", options=HIST_YEARS + [CURRENT_YEAR], index=len(HIST_YEARS), key="copy_to_y")
    overwrite = c3.checkbox("Sobrescribir líneas existentes en destino", value=False)
    if st.button("📋 Copiar presupuesto"):
        if int(from_y) == int(to_y):
            st.error("El año origen y destino no pueden ser el mismo.")
        else:
            n = copy_expense_budget(int(from_y), int(to_y), actor=user, overwrite=bool(overwrite))
            st.success(f"Copiadas {n} líneas (INSERT OR IGNORE).")
            st.rerun()


# =======================

# =======================
# DASHBOARD · DIRECCIÓN + SIMULADOR (DEMO)
# =======================
if page == "Dashboard · Dirección":
    # Cambia esto si quieres que lo vea más gente:
    # require_role([ROLE_OWNER, ROLE_EMPLOYEE, ROLE_SALESPERSON])
    require_role([ROLE_OWNER])

    st.title("📊 Dashboard de Dirección")

    year = st.selectbox("Ejercicio (año)", options=[CURRENT_YEAR] + HIST_YEARS, index=0, key="dash_year")

    # Objetivo anual (para demo). Si lo queréis persistente en BD lo añadimos luego.
    with st.expander("⚙️ Objetivo anual (para % y conclusión)", expanded=True):
        objetivo = st.number_input(
            "Objetivo de beneficio anual (€)",
            min_value=0.0,
            step=1000.0,
            value=0.0,
            key="dash_objetivo",
        )

    # ===== Datos base =====
    df_sales, monthly_sales, by_salesperson = signed_sales_monthly_summary(int(year))
    exp_monthly = expense_forecast_monthly_summary(int(year))

    monthly_full = pd.DataFrame({"period": [f"{int(year)}-{m}" for m in MONTHS]})
    monthly_full = monthly_full.merge(monthly_sales, on="period", how="left").fillna({"total": 0.0})
    monthly_full = monthly_full.merge(exp_monthly, on="period", how="left").fillna({"expenses": 0.0})

    monthly_full["ventas_firmadas"] = monthly_full["total"].astype(float)
    monthly_full["gastos_previstos"] = monthly_full["expenses"].astype(float)
    monthly_full["beneficio_previsto"] = (monthly_full["ventas_firmadas"] - monthly_full["gastos_previstos"]).round(2)
    monthly_full["beneficio_acumulado"] = monthly_full["beneficio_previsto"].cumsum().round(2)

    # ===== Formateadores =====
    def fmt0(x: float) -> str:
        return f"{x:,.0f} €".replace(",", "X").replace(".", ",").replace("X", ".")

    def fmt2(x: float) -> str:
        return f"{x:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")

    def fmt_delta(x: float) -> str:
        return f"{x:+,.0f} €".replace(",", "X").replace(".", ",").replace("X", ".")

    # =======================
    # SIMULADOR (WHAT-IF)
    # =======================
    st.subheader("🧪 Simulador (What-if)")

    cA, cB, cC, cD = st.columns([1.2, 1.2, 1.6, 1.0])

    # Reset demo (útil para presentar)
    if cD.button("Reset", use_container_width=True):
        st.session_state["sim_ventas_pct"] = 0
        st.session_state["sim_gastos_pct"] = 0
        st.session_state["sim_modo"] = "Solo meses futuros"
        st.rerun()

    ventas_factor = cA.slider(
        "Ajuste ventas (%)",
        min_value=-50,
        max_value=50,
        value=st.session_state.get("sim_ventas_pct", 0),
        step=1,
        key="sim_ventas_pct",
        help="Simula subir o bajar ventas previstas",
    )
    gastos_factor = cB.slider(
        "Ajuste gastos (%)",
        min_value=-50,
        max_value=50,
        value=st.session_state.get("sim_gastos_pct", 0),
        step=1,
        key="sim_gastos_pct",
        help="Simula subir o bajar gastos previstos",
    )
    modo = cC.radio(
        "Aplicar sobre",
        options=["Todo el año", "Solo meses futuros"],
        index=1 if st.session_state.get("sim_modo", "Solo meses futuros") == "Solo meses futuros" else 0,
        horizontal=True,
        key="sim_modo",
        help="En demo suele ser útil 'Solo meses futuros'",
    )

    # Copia para simular
    sim = monthly_full.copy()

    from datetime import date
    today = date.today()
    current_period = f"{today.year}-{today.month:02d}"

    def is_future(period: str) -> bool:
        # period formato YYYY-MM
        return str(period) >= current_period

    ventas_mult = 1.0 + (ventas_factor / 100.0)
    gastos_mult = 1.0 + (gastos_factor / 100.0)

    # Máscara: solo meses futuros si es el año actual y modo "Solo meses futuros"
    if modo == "Solo meses futuros" and int(year) == today.year:
        mask = sim["period"].apply(is_future)
    else:
        mask = pd.Series([True] * len(sim))

    sim.loc[mask, "ventas_firmadas"] = (sim.loc[mask, "ventas_firmadas"] * ventas_mult).round(2)
    sim.loc[mask, "gastos_previstos"] = (sim.loc[mask, "gastos_previstos"] * gastos_mult).round(2)

    sim["beneficio_previsto"] = (sim["ventas_firmadas"] - sim["gastos_previstos"]).round(2)
    sim["beneficio_acumulado"] = sim["beneficio_previsto"].cumsum().round(2)

    # Lo que se muestra (simulado)
    monthly_view = sim

    # KPIs base vs simulado (muy potente en demo)
    beneficio_base = float(monthly_full["beneficio_previsto"].sum())
    beneficio_sim = float(monthly_view["beneficio_previsto"].sum())
    delta_benef = beneficio_sim - beneficio_base

    k1, k2, k3 = st.columns(3)
    k1.metric("Beneficio (base)", fmt0(beneficio_base))
    k2.metric("Beneficio (simulado)", fmt0(beneficio_sim), delta=fmt_delta(delta_benef))
    k3.metric("Impacto simulación", fmt_delta(delta_benef))

    st.caption("Mueve los sliders: se recalculan KPIs, alertas y gráficos.")

    st.divider()

    # =======================
    # MÉTRICAS (impacto en demo)
    # =======================
    ventas_total = float(monthly_view["ventas_firmadas"].sum())
    gastos_total = float(monthly_view["gastos_previstos"].sum())
    beneficio_total = float(monthly_view["beneficio_previsto"].sum())

    # Predicción simple (run-rate) usando meses con actividad
    active = monthly_view[(monthly_view["ventas_firmadas"] > 0) | (monthly_view["gastos_previstos"] > 0)].copy()
    months_active = int(active.shape[0])
    if months_active > 0:
        avg_month_profit = float(active["beneficio_previsto"].mean())
        pred_fin_anyo = round(avg_month_profit * 12, 2)
    else:
        avg_month_profit = 0.0
        pred_fin_anyo = 0.0

    # Break-even
    be = monthly_view[monthly_view["beneficio_acumulado"] > 0]
    break_even_month = be.iloc[0]["period"] if not be.empty else None

    # Cumplimiento
    cumplimiento = (pred_fin_anyo / objetivo * 100.0) if objetivo and objetivo > 0 else None

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ventas previstas", fmt0(ventas_total))
    c2.metric("Gastos previstos", fmt0(gastos_total))
    c3.metric("Beneficio previsto", fmt0(beneficio_total))
    c4.metric("Predicción fin de año", fmt0(pred_fin_anyo))

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Meses con datos", str(months_active))
    c6.metric("Beneficio medio/mes", fmt2(avg_month_profit))
    c7.metric("Break-even estimado", break_even_month or "—")
    c8.metric("Cumplimiento objetivo", "—" if cumplimiento is None else f"{cumplimiento:.1f} %")

    st.divider()

    # =======================
    # CONCLUSIÓN AUTOMÁTICA (vende mucho)
    # =======================
    st.subheader("Conclusión automática")
    if objetivo and objetivo > 0:
        if pred_fin_anyo >= objetivo:
            st.success("✅ Con la previsión actual, se alcanzaría o superaría el objetivo anual.")
        elif pred_fin_anyo >= objetivo * 0.9:
            st.warning("🟠 Estás cerca del objetivo; conviene vigilar meses en rojo y ajustar.")
        else:
            st.error("🔴 Con la previsión actual, no se alcanzaría el objetivo; habría que ajustar ventas o gastos.")
    else:
        if pred_fin_anyo > 0:
            st.success("✅ La tendencia anual estimada es positiva.")
        elif pred_fin_anyo == 0:
            st.warning("🟠 Aún no hay datos suficientes para estimar tendencia anual.")
        else:
            st.error("🔴 La tendencia anual estimada es negativa.")

    # =======================
    # ALERTAS
    # =======================
    st.subheader("Alertas")
    meses_rojos = monthly_view[monthly_view["beneficio_previsto"] < 0][["period", "beneficio_previsto"]]
    if not meses_rojos.empty:
        st.warning("Meses con pérdida prevista (beneficio < 0):")
        st.dataframe(meses_rojos, use_container_width=True)
    else:
        st.success("No se detectan meses con pérdida prevista.")

    st.divider()

    # =======================
    # GRÁFICOS (simulados)
    # =======================
    st.subheader("Evolución mensual (ventas vs gastos vs beneficio)")
    st.line_chart(
        monthly_view.set_index("period")[["ventas_firmadas", "gastos_previstos", "beneficio_previsto"]],
        use_container_width=True,
    )

    st.subheader("Beneficio acumulado")
    st.line_chart(
        monthly_view.set_index("period")[["beneficio_acumulado"]],
        use_container_width=True,
    )

    st.divider()

    # =======================
    # RANKING COMERCIALES (robusto)
    # =======================
    st.subheader("🏆 Ranking comerciales (ventas firmadas)")
    if by_salesperson is None or by_salesperson.empty:
        st.info("Aún no hay ventas firmadas para mostrar ranking.")
    else:
        cols = list(by_salesperson.columns)

        name_col = None
        for c in ["salesperson", "vendedor", "comercial", "usuario", "user", "name"]:
            if c in cols:
                name_col = c
                break

        total_col = None
        for c in ["total", "importe", "amount", "ventas"]:
            if c in cols:
                total_col = c
                break

        if name_col and total_col:
            rank_df = by_salesperson[[name_col, total_col]].copy()
            rank_df[total_col] = rank_df[total_col].astype(float)
            rank_df = rank_df.sort_values(total_col, ascending=False)

            st.dataframe(rank_df, use_container_width=True)
            st.bar_chart(rank_df.set_index(name_col)[total_col], use_container_width=True)
        else:
            st.info("No he podido detectar columnas automáticamente. Muestro tabla completa:")
            st.dataframe(by_salesperson, use_container_width=True)

    st.divider()

    # =======================
    # TABLA + EXPORT
    # =======================
    st.subheader("Detalle mensual (exportable)")
    show_df = monthly_view[
        ["period", "ventas_firmadas", "gastos_previstos", "beneficio_previsto", "beneficio_acumulado"]
    ].copy()
    st.dataframe(show_df, use_container_width=True)

    csv = show_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "⬇️ Descargar CSV",
        data=csv,
        file_name=f"dashboard_direccion_{year}.csv",
        mime="text/csv",
    )


# ADMIN: MODELOS (crear + editar + renombrar código)
# =======================
if page == "Administrador · Modelos y precios":
    require_role([ROLE_OWNER])
    st.subheader("Modelos y precios (catálogo)")

    mdf_all = models_df(active_only=False)
    st.dataframe(mdf_all, use_container_width=True)

    st.divider()
    tab1, tab2 = st.tabs(["➕ Crear modelo", "✏️ Editar / Renombrar modelo"])

    with tab1:
        with st.form("create_model", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns(4)
            code = c1.text_input("Código", placeholder="303")
            name = c2.text_input("Nombre", placeholder="Estufa pellet ...")
            fuel = c3.selectbox("Combustible", ["pellet", "leña"])
            price = c4.number_input("Tarifa (€)", min_value=0.0, step=10.0, value=1500.0)
            active = st.checkbox("Activo", value=True)

            if st.form_submit_button("Crear"):
                if not code.strip() or not name.strip():
                    st.error("Código y nombre son obligatorios.")
                else:
                    try:
                        with engine.begin() as conn:
                            conn.execute(text("""
                                INSERT INTO models(code, name, fuel, list_price, active)
                                VALUES(:c,:n,:f,:p,:a)
                            """), {
                                "c": code.strip(),
                                "n": name.strip(),
                                "f": fuel,
                                "p": float(price),
                                "a": 1 if active else 0
                            })
                        st.success("Modelo creado.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"No se pudo crear (¿código duplicado?). Detalle: {e}")

    with tab2:
        mdf_all = models_df(active_only=False)
        if mdf_all.empty:
            st.info("No hay modelos.")
        else:
            mdf_all = mdf_all.copy()

            # Selección por ID (estable)
            ids = mdf_all["id"].astype(int).tolist()
            id_to_label = {
                int(r["id"]): f"{r['code']} — {r['name']} (id={int(r['id'])})"
                for _, r in mdf_all.iterrows()
            }

            selected_id = st.selectbox(
                "Selecciona modelo",
                options=ids,
                format_func=lambda i: id_to_label.get(int(i), str(i)),
                key="model_edit_id",
            )

            row = mdf_all[mdf_all["id"] == int(selected_id)].iloc[0]
            model_id = int(row["id"])

            st.markdown("### Editar datos del modelo")
            with st.form("form_edit_model", clear_on_submit=False):
                c1, c2, c3, c4 = st.columns(4)
                c1.text_input("Código actual", value=str(row["code"]), disabled=True)
                name = c2.text_input("Nombre", value=str(row["name"]))
                fuel = c3.selectbox(
                    "Combustible",
                    ["pellet", "leña"],
                    index=0 if row["fuel"] == "pellet" else 1,
                )
                price = c4.number_input(
                    "Tarifa (€)",
                    min_value=0.0,
                    step=10.0,
                    value=float(row["list_price"]),
                )
                active = st.checkbox("Activo", value=bool(row["active"]))

                if st.form_submit_button("💾 Guardar cambios"):
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE models
                            SET name=:n, fuel=:f, list_price=:p, active=:a
                            WHERE id=:id
                        """), {
                            "n": name.strip(),
                            "f": fuel,
                            "p": float(price),
                            "a": 1 if active else 0,
                            "id": model_id,
                        })
                    st.success("Cambios guardados.")
                    st.rerun()

            st.divider()
            st.markdown("### Renombrar código (si te equivocaste)")
            with st.form("form_rename_code", clear_on_submit=False):
                new_code = st.text_input("Nuevo código", value=str(row["code"]))
                if st.form_submit_button("🔁 Cambiar código"):
                    new_code = new_code.strip()
                    if not new_code:
                        st.error("El nuevo código no puede estar vacío.")
                    else:
                        with engine.begin() as conn:
                            exists = conn.execute(
                                text("SELECT 1 FROM models WHERE code=:c AND id<>:id"),
                                {"c": new_code, "id": model_id},
                            ).fetchone()

                            if exists:
                                st.error("Ese código ya existe en otro modelo.")
                                st.stop()

                            conn.execute(
                                text("UPDATE models SET code=:c WHERE id=:id"),
                                {"c": new_code, "id": model_id},
                             )

                            # ✅ VERIFICACIÓN: leo inmediatamente lo que quedó guardado
                            chk = conn.execute(
                                text("SELECT code FROM models WHERE id=:id"),
                                {"id": model_id},
                             ).fetchone()

                    st.success(f"Código actualizado en BD: {chk[0] if chk else '??'}")
                    st.rerun()

# =======================
# ADMIN: BANDEJA PDFs
# =======================
if page == "Administrador · Bandeja de PDFs":
    require_role([ROLE_OWNER])
    st.subheader("Bandeja de PDFs (validación y aprobación)")

    c1, c2, c3, c4 = st.columns(4)
    year_f = c1.selectbox("Año", options=[CURRENT_YEAR] + HIST_YEARS, index=0)
    scope_f = c2.selectbox("Scope", options=["(todos)", "CURRENT", "HISTORICAL"], index=0)
    status_f = c3.selectbox("Estado", options=["(todos)", "pending_validation", "approved", "error", "rejected"], index=0)
    doc_f = c4.selectbox("Tipo", options=["(todos)"] + DOC_TYPES, index=0)

    q = """
    SELECT id, uploaded_at, uploaded_by, doc_type, year, scope, period, bank, status, parse_log
    FROM documents
    WHERE year=:y
    """
    params = {"y": int(year_f)}

    if scope_f != "(todos)":
        q += " AND scope=:sc"
        params["sc"] = scope_f
    if status_f != "(todos)":
        q += " AND status=:st"
        params["st"] = status_f
    if doc_f != "(todos)":
        q += " AND doc_type=:dt"
        params["dt"] = doc_f

    q += " ORDER BY id DESC LIMIT 300"
    docs = read_df(q, params)
    st.dataframe(docs, use_container_width=True)

    st.divider()
    default_id = int(docs["id"].iloc[0]) if not docs.empty else 1
    doc_id = st.number_input("Abrir documento por ID", min_value=1, step=1, value=default_id)

    doc = read_df("SELECT * FROM documents WHERE id=:id", {"id": int(doc_id)})
    if doc.empty:
        st.info("Introduce un ID existente.")
        st.stop()

    d = doc.iloc[0].to_dict()
    st.markdown(f"""
**Documento #{d['id']}**  
- Tipo: `{d['doc_type']}` | Año: `{d['year']}` | Scope: `{d['scope']}` | Periodo: `{d['period']}` | Banco: `{d['bank']}`  
- Estado: `{d['status']}`  
- Subido por: `{d['uploaded_by']}` el `{d['uploaded_at']}`  
- Log: {d['parse_log']}
""")

    rows = read_df("""
        SELECT id, row_type, row_key, row_date, description, amount, currency
        FROM extracted_rows
        WHERE document_id=:id
        ORDER BY id ASC
    """, {"id": int(doc_id)})

    st.subheader("Filas detectadas")
    st.dataframe(rows, use_container_width=True)

    c1, c2, c3 = st.columns(3)
    if c1.button("✅ Aprobar"):
        with engine.begin() as conn:
            conn.execute(text("UPDATE documents SET status='approved' WHERE id=:id"), {"id": int(doc_id)})
        st.success("Aprobado.")
        st.rerun()

    if c2.button("❌ Rechazar"):
        with engine.begin() as conn:
            conn.execute(text("UPDATE documents SET status='rejected' WHERE id=:id"), {"id": int(doc_id)})
        st.warning("Rechazado.")
        st.rerun()

    if c3.button("🔁 Reprocesar"):
        rows_new, log = spot_document(d["doc_type"], d["filepath"])
        clear_extracted_rows(int(doc_id))
        insert_extracted_rows(int(doc_id), rows_new)
        new_status = "pending_validation" if rows_new else "error"
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE documents SET status=:s, parse_log=:l WHERE id=:id"),
                {"s": new_status, "l": log, "id": int(doc_id)},
            )
        st.info(log)
        st.rerun()


# =======================
# ADMIN: USUARIOS (ver PIN + eliminar + crear + cambiar PIN)
# =======================
if page == "Administrador · Usuarios":
    require_role([ROLE_OWNER])
    st.subheader("Usuarios (PIN simple)")

    users_df = read_df("SELECT username, role, pin FROM users ORDER BY role, username")
    if users_df.empty:
        st.info("No hay usuarios.")
    else:
        users_df = users_df.copy()
        users_df["Rol"] = users_df["role"].map(ROLE_LABELS).fillna(users_df["role"])
        st.dataframe(users_df[["username", "Rol", "pin"]], use_container_width=True)

    st.divider()
    st.subheader("Crear usuario")
    with st.form("create_user", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        u_new = c1.text_input("Usuario")
        pin_new = c2.text_input("PIN")
        role_new = c3.selectbox(
            "Rol",
            [ROLE_COMMERCIAL, ROLE_EMPLOYEE, ROLE_OWNER],
            format_func=lambda r: ROLE_LABELS.get(r, r)
        )
        if st.form_submit_button("Crear"):
            if not u_new.strip() or not pin_new.strip():
                st.error("Usuario y PIN obligatorios.")
            else:
                try:
                    with engine.begin() as conn:
                        conn.execute(
                            text("INSERT INTO users(username, pin, role) VALUES(:u,:p,:r)"),
                            {"u": u_new.strip(), "p": pin_new.strip(), "r": role_new},
                        )
                    st.success("Usuario creado.")
                    st.rerun()
                except Exception as e:
                    st.error(f"No se pudo crear (¿usuario duplicado?). Detalle: {e}")

    st.divider()
    st.subheader("Cambiar PIN")
    all_users = read_df("SELECT username FROM users ORDER BY username")
    u_options = all_users["username"].tolist() if not all_users.empty else []
    with st.form("reset_pin", clear_on_submit=True):
        c1, c2 = st.columns(2)
        u_ch = c1.selectbox("Usuario", options=u_options)
        pin_ch = c2.text_input("Nuevo PIN")
        if st.form_submit_button("Actualizar PIN"):
            if not pin_ch.strip():
                st.error("El PIN no puede estar vacío.")
            else:
                with engine.begin() as conn:
                    conn.execute(
                        text("UPDATE users SET pin=:p WHERE username=:u"),
                        {"p": pin_ch.strip(), "u": u_ch},
                    )
                st.success("PIN actualizado.")
                st.rerun()

    st.divider()
    st.subheader("Eliminar usuario")
    if not u_options:
        st.info("No hay usuarios para eliminar.")
    else:
        deletables = [u for u in u_options if u != "owner"]
        if not deletables:
            st.info("Solo existe 'owner'. No se puede eliminar.")
        else:
            u_del = st.selectbox("Usuario a eliminar", options=deletables)
            confirm = st.checkbox("Confirmo que quiero eliminar este usuario (acción irreversible)")
            if st.button("🗑️ Eliminar usuario"):
                if not confirm:
                    st.error("Marca la confirmación para eliminar.")
                else:
                    with engine.begin() as conn:
                        conn.execute(text("DELETE FROM users WHERE username=:u"), {"u": u_del})
                    st.success(f"Usuario eliminado: {u_del}")
                    st.rerun()