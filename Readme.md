# Como ejecutar (código para funcionamiento en local)

Primero se debe iniciar un entorno virtual, posteriormente en éste se pone el siguiente comando para instalar las dependencias con el requirements

```powershell
pip install -r requirements.txt
```

Luego se deben abrir cuatro terminales.

En la primera terminal se monta el master.py con el siguiente comando

```powershell
uvicorn master:app --host 127.0.0.1 --port 8000 --reload
```

En la segunda y tercera terminal se montan los nodos workers con los siguientes comandos

```powershell
#Se configuran las variables de entorno (URL del master y id del worker)
$env:MASTER_URL = "http://127.0.0.1:8000"
$env:WORKER_ID  = "w1"
python .\worker.py
```

```powershell
$env:MASTER_URL = "http://127.0.0.1:8000"
$env:WORKER_ID  = "w2"
python .\worker.py
```

Finalmente en la cuarta terminal se contacta al master por medio de API-REST

```powershell
curl.exe -X POST "http://127.0.0.1:8000/jobs" -H "Content-Type: application/json" --data-binary "@shared/jobs/job-request.json"
```