package org.transito_seguro.model.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.springframework.beans.factory.annotation.Autowired;
import org.transito_seguro.component.analyzer.QueryAnalyzer;
import org.transito_seguro.component.analyzer.SqlParser;
import org.transito_seguro.enums.EstadoQuery;
import org.transito_seguro.enums.EstrategiaPaginacion;
import org.transito_seguro.enums.TipoConsolidacion;
import org.transito_seguro.model.FiltroMetadata;

import javax.persistence.*;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Entity
@Table(name = "query_storage", indexes = {
        @Index(name = "idx_query_codigo", columnList = "codigo", unique = true),
        @Index(name = "idx_query_categoria", columnList = "categoria"),
        @Index(name = "idx_query_activa", columnList = "activa")
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class QueryStorage {

    @Transient
    @Autowired
    private SqlParser sqlParser;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    // Identificador único de la query
    @Column(name = "codigo", unique = true, nullable = false, length = 100)
    private String codigo;

    @Column(name = "nombre", nullable = false, length = 200)
    private String nombre;

    @Column(name = "descripcion", columnDefinition = "TEXT")
    private String descripcion;

    // Query SQL almacenada
    @Column(name = "sql_query", columnDefinition = "TEXT", nullable = false)
    private String sqlQuery;

    @Column(name = "categoria", length = 50)
    private String categoria;

    // Metadata de consolidación
    /**Indica si la query esta diseñada para ser consolidable */
    @Column(name = "es_consolidable", nullable = false)
    private Boolean esConsolidable = false;

    @Column(name = "tipo_consolidacion")
    @Enumerated(EnumType.STRING)
    private TipoConsolidacion tipoConsolidacion;

    @Column(name = "estrategia_paginacion")
    @Enumerated(EnumType.STRING)
    private EstrategiaPaginacion estrategiaPaginacion;

    @Column(name = "campos_agrupacion", columnDefinition = "TEXT")
    private String camposAgrupacion;

    @Column(name = "campos_numericos", columnDefinition = "TEXT")
    private String camposNumericos;

    @Column(name = "campos_ubicacion", columnDefinition = "TEXT")
    private String camposUbicacion;

    @Column(name = "campos_tiempo", columnDefinition = "TEXT")
    private String camposTiempo;

    
    @Column(name = "version", nullable = false)
    @Builder.Default  
    private Integer version = 1;

    @Column(name = "activa", nullable = false)
    @Builder.Default  
    private Boolean activa = true;

    @Enumerated(EnumType.STRING)
    @Column(name = "estado")
    @Builder.Default  
    private EstadoQuery estado = EstadoQuery.PENDIENTE;

    // Auditoría
    @Column(name = "creado_por", length = 100)
    private String creadoPor;

    @Column(name = "fecha_creacion", nullable = false)
    private LocalDateTime fechaCreacion;

    @Column(name = "fecha_actualizacion")
    private LocalDateTime fechaActualizacion;

    @Column(name = "ultimo_uso")
    private LocalDateTime ultimoUso;

    @Column(name = "contador_usos")
    @Builder.Default  
    private Long contadorUsos = 0L;

    // Configuración de ejecución
    @Column(name = "timeout_segundos")
    @Builder.Default  
    private Integer timeoutSegundos = 30;


    //persiste en la base de datos
    @Column(length = 500)
    private String filtrosJson;

    @Transient
    private Map<String, FiltroMetadata> camposFiltros;


    /**
     * ObjectMapper para conversiones JSON.
     * Se reutiliza para evitar crear instancias repetidas.
     */
    @Transient
    private static final ObjectMapper objectMapper = new ObjectMapper();



    /*
     * Si la estimacion es null se calcula de forma dinamica 
     * 10.000 < consulta directa
     * 10.000 >  usar streaming
     * Usando para decidir que estrategia de usar
     * 
     */
    @Column(name = "registros_estimados")
    private Integer registrosEstimados;

    @Column(name = "limite_maximo")
    @Builder.Default  
    private Integer limiteMaximo = 50000;

    // Tags para búsqueda y organización
    @Column(name = "tags", length = 500)
    private String tags;

    // =============== MÉTODOS DE CONVENIENCIA ===============

    /**
     * Obtiene campos de agrupación como lista
     */
    public List<String> getCamposAgrupacionList() {
        return org.transito_seguro.utils.JsonUtils.fromJsonToList(this.camposAgrupacion);
    }

    /**
     * Establece campos de agrupación desde lista
     */
    public void setCamposAgrupacionList(List<String> campos) {
        this.camposAgrupacion = org.transito_seguro.utils.JsonUtils.toJson(campos);
    }

    /**
     * Obtiene campos numéricos como lista
     */
    public List<String> getCamposNumericosList() {
        return org.transito_seguro.utils.JsonUtils.fromJsonToList(this.camposNumericos);
    }

    /**
     * Establece campos numéricos desde lista
     */
    public void setCamposNumericosList(List<String> campos) {
        this.camposNumericos = org.transito_seguro.utils.JsonUtils.toJson(campos);
    }

    /**
     * Obtiene campos de ubicación como lista
     */
    public List<String> getCamposUbicacionList() {
        return org.transito_seguro.utils.JsonUtils.fromJsonToList(this.camposUbicacion);
    }

    /**
     * Establece campos de ubicación desde lista
     */
    public void setCamposUbicacionList(List<String> campos) {
        this.camposUbicacion = org.transito_seguro.utils.JsonUtils.toJson(campos);
    }

    /**
     * Obtiene campos de tiempo como lista
     */
    public List<String> getCamposTiempoList() {
        return org.transito_seguro.utils.JsonUtils.fromJsonToList(this.camposTiempo);
    }

    /**
     * Establece campos de tiempo desde lista
     */
    public void setCamposTiempoList(List<String> campos) {
        this.camposTiempo = org.transito_seguro.utils.JsonUtils.toJson(campos);
    }

    /**
     * Registra uso de la query
     */
    public void registrarUso() {
        this.ultimoUso = LocalDateTime.now();
        this.contadorUsos = this.contadorUsos != null ? this.contadorUsos + 1 : 1L;
    }

    /**
     * Verifica si la query está lista para usar
     */
    public boolean estaLista() {
        return this.activa && this.estado != EstadoQuery.ERROR &&
                this.sqlQuery != null && !this.sqlQuery.trim().isEmpty();
    }

    /**
     * Obtiene tags como lista
     */
    public List<String> getTagsList() {
        if (this.tags == null || this.tags.trim().isEmpty()) {
            return java.util.Collections.emptyList();
        }
        return java.util.Arrays.asList(this.tags.split(","))
                .stream()
                .map(String::trim)
                .filter(tag -> !tag.isEmpty())
                .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Establece tags desde lista
     */
    public void setTagsList(List<String> tagsList) {
        if (tagsList == null || tagsList.isEmpty()) {
            this.tags = null;
        } else {
            this.tags = String.join(",", tagsList);
        }
    }

    @PrePersist
    protected void onCreate() {
        this.fechaCreacion = LocalDateTime.now();
        this.fechaActualizacion = LocalDateTime.now();

        // Asegurar valores por defecto en @PrePersist
        if (this.version == null) {
            this.version = 1;
        }
        if (this.activa == null) {
            this.activa = true;
        }
        if (this.estado == null) {
            this.estado = EstadoQuery.PENDIENTE;
        }
        if (this.esConsolidable == null) {
            this.esConsolidable = false;
        }
        if (this.contadorUsos == null) {
            this.contadorUsos = 0L;
        }

        // Convertir el Map a JSON
        convertirMapAJson();
    }


    public void analizarYMarcarConsolidable() {
        if (this.sqlQuery != null) {
            boolean tieneGroupBy = SqlParser.tieneGroupBy(this.sqlQuery);
            if (tieneGroupBy) {
                this.esConsolidable = true;
            }
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.fechaActualizacion = LocalDateTime.now();
        // Convertir el Map a JSON
        convertirMapAJson();
    }

    /**
     * Se ejecuta antes de INSERT o UPDATE en la base de datos.
     * Convierte el Map de filtros en memoria a JSON para persistir.
     *
     * Si camposFiltros es null o está vacío, guarda null en BD.
     */
    private void convertirMapAJson() {
        try {
            if (camposFiltros == null || camposFiltros.isEmpty()) {
                this.filtrosJson = null;
            } else {
                this.filtrosJson = objectMapper.writeValueAsString(camposFiltros);
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error al convertir filtros a JSON", e);
        }
    }

    /**
     * Se ejecuta después de cargar la entidad desde la base de datos.
     * Convierte el JSON de filtros a Map para usar en memoria.
     *
     * Si filtrosJson es null o vacío, inicializa un Map vacío.
     */
    @PostLoad
    public void convertirJsonAMap() {

        try {
            if (filtrosJson == null || filtrosJson.trim().isEmpty()) {
                this.camposFiltros = new HashMap<>();

            } else {
                this.camposFiltros = objectMapper.readValue(
                        filtrosJson,
                        new TypeReference<Map<String, FiltroMetadata>>() {}
                );

            }
        } catch (IOException e) {

            // Inicializar Map vacío para evitar NPE
            this.camposFiltros = new HashMap<>();
        }
    }

}