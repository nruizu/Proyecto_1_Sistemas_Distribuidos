# GridMR – MapReduce en arquitectura Maestro–Trabajadores

## Información general

* **Materia:** ST0263 Tópicos Especiales en Telemática / SI3007 Sistemas Distribuidos
* **Estudiantes:** 

  * Vladlen Shatunov – [vshatunov@eafit.edu.co](mailto:vshatunov@eafit.edu.co)
  * Nicolás Ruiz Urrea – [nruizu@eafit.edu.co](mailto:nruizu@eafit.edu.co)
  * Laura Valentina Ortiz Caballero – [lortizc3@eafit.edu.co](mailto:lortizc3@eafit.edu.co)
* **Profesor:** Edwin Nelson Montoya Munera – emontoya@eafit.edu.co

---

## 1. Breve descripción del proyecto

Este proyecto implementa un sistema distribuido **Maestro–Trabajadores** para ejecutar trabajos de tipo **MapReduce** sobre archivos de texto en un entorno con almacenamiento compartido.
Se utiliza un caso de uso de **WordCount**, donde se cuentan las palabras presentes en los archivos `data_1.txt` y `data_2.txt`.

El Maestro está implementado con **FastAPI** y coordina Workers que ejecutan las funciones `map` y `reduce` definidas en `job.py`. Los resultados se consolidan en un archivo `Final.txt`.

### 1.1 Aspectos cumplidos

* Arquitectura Maestro–Trabajadores funcional.
* API REST implementada en el Maestro con FastAPI.
* Procesamiento distribuido en fases MAP, SHUFFLE y REDUCE.
* Creación de splits (20 líneas por defecto).
* Planificación round-robin de tareas para balanceo de carga.
* Generación de resultados finales en `Final.txt`.

### 1.2 Aspectos no cumplidos

* No se implementó MOM (Message-Oriented Middleware).

* No se implementó el demon.

* En el video demostrativo no se conectó a las instancias mediante NFS.

---

## 2. Diseño de alto nivel y arquitectura

* **Patrón:** Maestro–Trabajadores.
* **Procesamiento:** modelo clásico de MapReduce.
* **Comunicación:** API REST vía HTTP.
* **Datos:** almacenados en carpeta `shared/` como almacenamiento común.

**Componentes principales:**

* `master.py`: Maestro, coordina trabajos y controla fases MAP → REDUCE.
* `worker.py`: ejecuta las funciones `map` y `reduce`.
* `job.py`: define funciones `map(line)` y `reduce(key, values)`.
* `shared/jobs/job-request.json`: especifica parámetros del job.
* `shared/input/`: datasets de prueba.

**Estructura de directorios:**

```bash
.
├── job.py
├── master.py
├── worker.py
├── Readme.md
├── requirements.txt
├── shared/
│   ├── input/
│   │   ├── data_1.txt
│   │   └── data_2.txt
│   ├── jobs/
│   │   ├── job.py
│   │   └── job-request.json
│   ├── splits/
│   ├── intermediate_map/
│   ├── intermediate_reduce/
│   └── output/
```

---

## 3. Ambiente de desarrollo y técnico

* **Lenguaje:** Python 3.10+
* **Frameworks:** FastAPI (para el Maestro), requests (para Workers).
* **Dependencias (requirements.txt):**

  * fastapi
  * uvicorn
  * requests
  * python-dotenv
  * python-multipart

### 3.1 Cómo ejecutar (funcionamiento en local)

Primero se debe iniciar un entorno virtual. Posteriormente, instalar dependencias con el requirements:

```powershell
pip install -r requirements.txt
```

Luego se deben abrir cuatro terminales:

**1. Iniciar el Maestro:**

```powershell
uvicorn master:app --host 127.0.0.1 --port 8000 --reload
```

**2. Iniciar Worker 1:**

```powershell
$env:MASTER_URL = "http://127.0.0.1:8000"
$env:WORKER_ID  = "w1"
python .\worker.py
```

**3. Iniciar Worker 2:**

```powershell
$env:MASTER_URL = "http://127.0.0.1:8000"
$env:WORKER_ID  = "w2"
python .\worker.py
```

**4. Enviar el job al Maestro:**

```powershell
curl.exe -X POST "http://127.0.0.1:8000/jobs" -H "Content-Type: application/json" --data-binary "@shared/jobs/job-request.json"
```

---

## 4. Ambiente de ejecución (producción)

* **Lenguaje:** Python 3.10+
* **Frameworks:** FastAPI + uvicorn para Maestro, Python + requests para Workers.
* **Infraestructura recomendada:**

  * Maestro y Workers en contenedores Docker separados.
  * Carpeta `shared/` montada como volumen compartido.

### Cómo lanzar el servidor

```bash
uvicorn master:app --host 0.0.0.0 --port 8000
```

### Uso del sistema

1. Subir código `job.py` y datasets en `shared/`.
2. Configurar `job-request.json` con parámetros de ejecución.
3. Ejecutar Maestro y Workers.
4. Consultar resultados en `shared/output/job-<id>/Final.txt`.

---

## 5. Otra información relevante

* No incluye reintentos automáticos ni replicación de datos.
* Escalable: al aumentar Workers, mejora el tiempo de ejecución.

---

## Referencias

* Documentación oficial de FastAPI: [https://fastapi.tiangolo.com](https://fastapi.tiangolo.com)
* MapReduce: Dean, J., & Ghemawat, S. (2004). *MapReduce: Simplified Data Processing on Large Clusters*.
* Ejemplos de sistemas distribuidos vistos en clase.
