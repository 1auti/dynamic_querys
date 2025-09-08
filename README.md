# Sistema de Consulta Dinámica de Infracciones de Tránsito

Sistema de consultas avanzado para datos de infracciones de tránsito multi-provincia con soporte para múltiples formatos de exportación y procesamiento eficiente de grandes volúmenes de datos.

## 📋 Características Principales

- **Multi-provincia**: Soporte para múltiples bases de datos provinciales
- **Consultas dinámicas**: Sistema flexible de filtros parametrizados
- **Múltiples formatos**: Exportación en JSON, CSV y Excel
- **Procesamiento eficiente**: Manejo optimizado de grandes volúmenes de datos
- **API REST**: Endpoints bien estructurados para consulta y descarga
- **Streaming**: Procesamiento por lotes para archivos grandes

## 🗄️ Provincias Soportadas

| Provincia | Código | Base de Datos |
|-----------|--------|---------------|
| Buenos Aires | `pba` | `pba_db` |
| Avellaneda | `mda` | `mda_db` |
| La Pampa | `santa-rosa` | `santa_rosa_db` |
| Chaco | `chaco` | `chaco_db` |
| Entre Ríos | `entre-rios` | `entre_rios_db` |
| Formosa | `formosa` | `formosa_db` |

## 🚀 Instalación y Configuración

### Prerrequisitos

- Java 11 o superior
- Maven 3.6+
- PostgreSQL 12+
- Spring Boot 2.7+

### Variables de Entorno

```bash
# Base de datos Buenos Aires
PBA_URL=jdbc:postgresql://localhost:5432/pba_db
PBA_USERNAME=postgres
PBA_PASSWORD=password

# Base de datos Avellaneda
MDA_URL=jdbc:postgresql://localhost:5432/mda_db
MDA_USERNAME=postgres
MDA_PASSWORD=password

# Base de datos La Pampa
SANTA_ROSA_URL=jdbc:postgresql://localhost:5432/santa_rosa_db
SANTA_ROSA_USERNAME=postgres
SANTA_ROSA_PASSWORD=password

# Base de datos Chaco
CHACO_URL=jdbc:postgresql://localhost:5432/chaco_db
CHACO_USERNAME=postgres
CHACO_PASSWORD=password

# Base de datos Entre Ríos
ENTRE_RIOS_URL=jdbc:postgresql://localhost:5432/entre_rios_db
ENTRE_RIOS_USERNAME=postgres
ENTRE_RIOS_PASSWORD=password

# Base de datos Formosa
FORMOSA_URL=jdbc:postgresql://localhost:5432/formosa_db
FORMOSA_USERNAME=postgres
FORMOSA_PASSWORD=password

# Puerto del servidor (opcional)
SERVER_PORT=8080
```

### Ejecución

```bash
# Clonar el repositorio
git clone <repository-url>
cd transito-seguro

# Compilar
mvn clean compile

# Ejecutar
mvn spring-boot:run
```

## 📝 Tipos de Consultas Disponibles

| Tipo | Endpoint | Descripción |
|------|----------|-------------|
| `personas-juridicas` | `/api/infracciones/personas-juridicas` | Consulta de personas jurídicas |
| `infracciones-general` | `/api/infracciones/infracciones-general` | Reporte general de infracciones |
| `infracciones-detallado` | `/api/infracciones/infracciones-detallado` | Reporte detallado de infracciones |
| `infracciones-por-equipos` | `/api/infracciones/infracciones-por-equipos` | Infracciones por equipos |
| `radar-fijo-por-equipo` | `/api/infracciones/radar-fijo-por-equipo` | Radar fijo por equipo |
| `semaforo-por-equipo` | `/api/infracciones/semaforo-por-equipo` | Semáforos por equipo |
| `vehiculos-por-municipio` | `/api/infracciones/vehiculos-por-municipio` | Vehículos por municipio |
| `sin-email-por-municipio` | `/api/infracciones/sin-email-por-municipio` | Infracciones sin email |
| `verificar-imagenes-radar` | `/api/infracciones/verificar-imagenes-radar` | Verificación de imágenes |

## 🔗 Endpoints Principales

### Consultas (Con límite automático de 5,000 registros)

```bash
# Consulta genérica
POST /api/infracciones/{tipoConsulta}

# Consultas específicas
POST /api/infracciones/personas-juridicas
POST /api/infracciones/infracciones-general
POST /api/infracciones/infracciones-detallado
# ... etc
```

### Descargas (Sin límite)

```bash
# Descarga genérica
POST /api/infracciones/{tipoConsulta}/descargar
POST /api/infracciones/descargar/{tipoConsulta}

# Descarga específica
POST /api/descargar/personas-juridicas
POST /api/descargar/reporte-general
# ... etc
```

### Información del Sistema

```bash
# Información de límites
GET /api/infracciones/limits-info
```

## 📄 Estructura de Request

### Ejemplo Básico

```json
{
  "formato": "json",
  "parametrosFiltros": {
    "fechaInicio": "2024-01-01",
    "fechaFin": "2024-12-31",
    "baseDatos": ["BuenosAires", "Entre Ríos"],
    "limite": 1000
  }
}
```

### Ejemplo Completo

```json
{
  "formato": "excel",
  "parametrosFiltros": {
    "tipoDocumento": "CUIT",
    "fechaEspecifica": "2024-06-15",
    "provincias": ["BuenosAires"],
    "municipios": ["La Plata", "Mar del Plata"],
    "baseDatos": ["pba"],
    "usarTodasLasBDS": false,
    "tieneEmail": true,
    "exportadoSacit": false,
    "limite": 5000,
    "offset": 0,
    "concesiones": [1, 2, 3],
    "tiposInfracciones": [1, 2],
    "estadosInfracciones": [20, 21],
    "patronesEquipos": ["SE", "VLR"],
    "tipoVehiculo": ["AUTO", "MOTO"]
  }
}
```

## 🔧 Parámetros de Filtrado

### Fechas
- `fechaInicio`: Fecha de inicio (YYYY-MM-DD)
- `fechaFin`: Fecha de fin (YYYY-MM-DD)
- `fechaEspecifica`: Fecha específica (YYYY-MM-DD)

### Ubicación
- `provincias`: Array de provincias
- `municipios`: Array de municipios
- `baseDatos`: Array de códigos de base de datos
- `usarTodasLasBDS`: Usar todas las bases (boolean)

### Infracciones
- `tiposInfracciones`: Array de IDs de tipos
- `estadosInfracciones`: Array de IDs de estados
- `exportadoSacit`: Exportado a SACIT (boolean)

### Equipos
- `patronesEquipos`: Patrones de búsqueda
- `tiposDispositivos`: Array de IDs de dispositivos
- `concesiones`: Array de IDs de concesiones

### Otros
- `tieneEmail`: Filtrar por email (boolean)
- `tipoVehiculo`: Array de tipos de vehículo
- `limite`: Límite de registros
- `offset`: Desplazamiento para paginación

## 📊 Formatos de Salida

### JSON (Predeterminado)
```json
{
  "datos": {
    "total": 1500,
    "datos": [...]
  },
  "metadata": {
    "limite_aplicado": 5000,
    "limite_fue_reducido": false
  }
}
```

### CSV
Archivo CSV con headers y datos separados por comas.

### Excel
Archivo XLSX con formato, headers en negrita y columnas auto-ajustadas.

## ⚡ Límites y Performance

### Consultas (Endpoints /api/infracciones/*)
- **Límite automático**: 5,000 registros
- **Procesamiento**: Síncrono optimizado
- **Uso**: Ideal para visualización en aplicaciones

### Descargas (Endpoints /descargar)
- **Sin límite**: Procesa todos los registros disponibles
- **Procesamiento**: Por lotes con streaming
- **Uso**: Ideal para exportación y análisis

### Configuración de Memoria
```yaml
app:
  batch:
    size: 10000
    chunk-size: 2000
    max-memory-per-batch: 200
    memory-critical-threshold: 0.80
```

## 🛠️ Ejemplos de Uso

### 1. Consultar Personas Jurídicas de Buenos Aires

```bash
curl -X POST "http://localhost:8080/api/infracciones/personas-juridicas" \
  -H "Content-Type: application/json" \
  -d '{
    "formato": "json",
    "parametrosFiltros": {
      "baseDatos": ["BuenosAires"],
      "tieneEmail": true,
      "limite": 1000
    }
  }'
```

### 2. Descargar Reporte Completo en Excel

```bash
curl -X POST "http://localhost:8080/api/descargar/reporte-general" \
  -H "Content-Type: application/json" \
  -d '{
    "formato": "excel",
    "parametrosFiltros": {
      "fechaInicio": "2024-01-01",
      "fechaFin": "2024-12-31",
      "usarTodasLasBDS": true
    }
  }' \
  --output reporte_infracciones.xlsx
```

### 3. Consultar por Rango de Fechas

```bash
curl -X POST "http://localhost:8080/api/infracciones/infracciones-detallado" \
  -H "Content-Type: application/json" \
  -d '{
    "formato": "json",
    "parametrosFiltros": {
      "fechaInicio": "2024-06-01",
      "fechaFin": "2024-06-30",
      "exportadoSacit": false,
      "baseDatos": ["pba", "mda"]
    }
  }'
```

## 🐛 Manejo de Errores

### Códigos de Error Comunes

| Código | Descripción | Solución |
|--------|-------------|----------|
| 400 | Parámetros inválidos | Verificar estructura del JSON |
| 404 | Tipo de consulta no encontrado | Usar tipos válidos listados arriba |
| 500 | Error interno | Verificar conexión a BD y logs |

### Ejemplo de Respuesta de Error

```json
{
  "error": "Validación fallida",
  "detalle": "La fecha de inicio debe ser anterior a la fecha fin",
  "timestamp": "2024-01-15T10:30:00.000Z",
  "status": 400
}
```

## 📈 Monitoreo

### Health Check
```bash
GET /actuator/health
```

### Métricas
```bash
GET /actuator/metrics
```

### Información de Límites
```bash
GET /api/infracciones/limits-info
```

## 🏗️ Arquitectura

### Componentes Principales

- **Controllers**: Manejo de endpoints REST
- **Services**: Lógica de negocio y orquestación
- **Repositories**: Acceso a datos multi-provincia
- **Components**: Procesamiento, validación y conversión
- **Factory**: Creación dinámica de repositorios

### Patrones Implementados

- **Factory Pattern**: Para repositorios multi-provincia
- **Strategy Pattern**: Para diferentes formatos de salida
- **Streaming Pattern**: Para procesamiento eficiente
- **Builder Pattern**: Para DTOs complejos

## 🔒 Seguridad

### Configuración CORS
```yaml
@CrossOrigin(origins = "*", maxAge = 3600)
```

### Validación de Entrada
- Validación automática con `@Valid`
- Sanitización de parámetros SQL
- Límites de memoria y tiempo

## 📚 Documentación Adicional

### Estructura de Base de Datos
Cada provincia debe tener las siguientes tablas principales:
- `dominios`
- `dominio_titulares`
- `infraccion`
- `concesion`
- `punto_control`
- `tipo_infraccion`

### Queries SQL
Las consultas SQL están ubicadas en `src/main/resources/querys/` y son cargadas dinámicamente.

## 🤝 Contribución

1. Fork el proyecto
2. Crear rama feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

## 📄 Licencia

Este proyecto está bajo la licencia [MIT](LICENSE).

## 📞 Soporte

Para soporte técnico o consultas:
- Email: soporte@transitoseguro.com
- Issues: GitHub Issues del proyecto

---

**Versión**: 1.0.0  
**Última actualización**: Enero 2024
