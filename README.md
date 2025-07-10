# 🔷 SmartBank Transactions Pipeline: ETL Streaming con Azure y Databricks

## 🧾 Descripción del Proyecto

Este proyecto implementa un pipeline **ETL en streaming** de transacciones financieras utilizando herramientas modernas de Azure y Apache Spark.  
Emplea una arquitectura **medallón (Bronze, Silver, Gold)** para asegurar calidad, trazabilidad y valor en cada capa del procesamiento de datos.

---

## 🏗️ Tecnologías Utilizadas

- **Azure Data Lake Storage (ADLS Gen2)**: almacenamiento escalable de datos crudos y transformados  
- **Azure Event Hub** : servicio de ingesta de datos en tiempo real 
- **Autoloader de Databricks**: ingestión continua de datos  
- **Databricks (PySpark Structured Streaming)**: procesamiento en tiempo casi real  
- **Delta Lake**: manejo de datos confiable, versionado y escalable  
- **Unity Catalog**: gobernanza de datos y control centralizado de acceso  
- **Databricks Secrets**: gestión segura de credenciales  

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
