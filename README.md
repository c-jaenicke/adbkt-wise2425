# adbkt-wise2425

## P2

### Installieren mariadb Connector

1. Portainer anmelden
2. Shell in Python Container öffnen
3. Venv aktivieren
4. Abhängigkeiten installieren mit `apt install -y libmariadb-dev gcc`
5. Connector installieren mit `uv pip install mariadb`

### Cred Datei

Name: `cred_mariap2.py`

```python
pg_userid = '...'
pg_password = '...'
pg_host = '...'
pg_db = '...'
pg_port = 3306
```
