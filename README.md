# 🔷 SmartBank Transactions Pipeline: ETL Streaming con Azure y Databricks

## 🧾 Descripción del Proyecto

Este proyecto implementa un pipeline **ETL en streaming** de transacciones financieras utilizando herramientas modernas de Azure y Apache Spark.  
Emplea una arquitectura **medallón (Bronze, Silver, Gold)** para asegurar calidad, trazabilidad y valor en cada capa del procesamiento de datos.
Finalmente Integré un **Data Warehouse** en Azure utilizando Azure SQL Database para almacenar los datos transformados y enriquecidos (capa Gold), facilitando su consumo desde herramientas como Power BI.


---

## 🏗️ Tecnologías Utilizadas

- **Azure Data Lake Storage (ADLS Gen2)**: almacenamiento escalable de datos crudos y transformados  
- **Azure Event Hub** : servicio de ingesta de datos en tiempo real 
- **Autoloader de Databricks**: ingestión continua de datos  
- **Databricks (PySpark Structured Streaming)**: procesamiento en tiempo casi real  
- **Delta Lake**: manejo de datos confiable, versionado y escalable  
- **Unity Catalog**: gobernanza de datos y control centralizado de acceso  
- **Databricks Secrets**: gestión segura de credenciales
- **Azure SQL Database**: arquitectura Warehouse

---

## 🧱 Arquitectura Medallón

### 🔹 Bronze Layer
- Ingesta continua de archivos JSON de transacciones utilizando Autoloader.
- Los datos se almacenan en formato **Delta**, sin modificaciones.
- Garantiza **trazabilidad** y recuperación ante fallos.

### 🔸 Silver Layer
- Limpieza y validaciones (monto > 0, campos obligatorios no nulos).
- Enriquecimiento de datos:
  - Derivación de columnas de fecha/hora
  - Normalización de texto
  - Clasificación de montos (`amount_level`)
  - Detección básica de fraudes
- Los datos se escriben como una **tabla Delta gestionada** en el **Unity Catalog**.

### 🏅 Gold Layer
Agregaciones y métricas clave por país y canal:
- Total de transacciones
- Suma total y promedio de montos
- Porcentaje de fraudes detectados
- Ratio de transacciones de alto valor (`high_value_ratio_percent`)
- Escritura en csv y  tablas delta en **Unity Catalog**

Todos estos indicadores son integrados en una tabla final:  
➡️ **`gold_transactions.gold.global_kpis`**

---

> 💼 Este pipeline demuestra una implementación de un sistema de datos moderno en la nube en tiempo casi real, con un recorrido de punta a punta automatico mediante **Schedule Trigger** (8:27 PM)



Screenshots / Capturas:

Script principal de ingesta a Azure eventHub


![image](https://github.com/user-attachments/assets/8fafbfb2-5651-4cfe-af52-0a2b41a16c97)
![image](https://github.com/user-attachments/assets/1d2cb5c2-5e24-4c32-bbd1-0b6d1a86cbe3)

Llegada de requests a Azure eventHub


![image](https://github.com/user-attachments/assets/6a528f67-db69-455b-b50a-30e464fcbf3a)

Pipeline Bronce y silver


![image](https://github.com/user-attachments/assets/f8e6390c-f97d-4b69-a976-1f5fc9c9e60d)

Checkpoints Silver


![image](https://github.com/user-attachments/assets/145f98a5-fd51-4b42-9758-e3aa38f15307)


Pipeline Gold y load a Azure SQL


![image](https://github.com/user-attachments/assets/9efddc03-6171-4edb-bfeb-3e5c363d15b7)

Verificaciones de registros nuevos en Azure SQL


![image](https://github.com/user-attachments/assets/bce2ae4a-9ef1-4979-b340-4632ce9b6848)

KPI's final (Gold)


![image](https://github.com/user-attachments/assets/c4dab88a-9d63-473f-b370-021496bff602)

