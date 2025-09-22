package org.transito_seguro.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;
import org.transito_seguro.enums.EstadoQuery;
import org.transito_seguro.utils.JsonUtils;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.List;

@Entity
@Table(name = "query_metadata")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class QueryMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "nombre_query", unique = true, nullable = false, length = 200)
    private String nombreQuery;

    @Column(name = "hash_query", nullable = false, length = 64)
    private String hashQuery; // Para detectar cambios en el contenido

    @Column(name = "es_registrada", nullable = false)
    private Boolean esRegistrada = false;

    @Column(name = "es_consolidable", nullable = false)
    private Boolean esConsolidable = false;

    // Campos de consolidación serializados como JSON
    @Column(name = "campos_agrupacion", columnDefinition = "TEXT")
    private String camposAgrupacion; // JSON: ["provincia", "municipio"]

    @Column(name = "campos_numericos", columnDefinition = "TEXT")
    private String camposNumericos; // JSON: ["total", "cantidad"]

    @Column(name = "campos_ubicacion", columnDefinition = "TEXT")
    private String camposUbicacion; // JSON: ["provincia", "contexto"]

    @Column(name = "campos_tiempo", columnDefinition = "TEXT")
    private String camposTiempo; // JSON: ["fecha", "fecha_reporte"]

    // Metadata adicional
    @Column(name = "descripcion", length = 500)
    private String descripcion;

    @Column(name = "categoria", length = 50)
    private String categoria; // INFRACCIONES, VEHICULOS, REPORTES, etc.

    @Column(name = "version_analisis", length = 10)
    private String versionAnalisis; // Para invalidar cache cuando cambie lógica

    // Campos de auditoría
    @Column(name = "fecha_creacion", nullable = false)
    private LocalDateTime fechaCreacion;

    @Column(name = "fecha_ultima_actualizacion")
    private LocalDateTime fechaUltimaActualizacion;

    @Column(name = "complejidad", length = 20)
    private String complejidad; // SIMPLE, MEDIA, COMPLEJA

    @Column(name = "fecha_ultimo_uso")
    private LocalDateTime fechaUltimoUso;

    // Campos para análisis automático
    @Column(name = "requiere_reanalizis", nullable = false)
    private Boolean requiereReanalisis = false;

    @Column(name = "error_ultimo_analisis", columnDefinition = "TEXT")
    private String errorUltimoAnalisis;

    // Estado de la query
    @Enumerated(EnumType.STRING)
    @Column(name = "estado", nullable = false)
    private EstadoQuery estado = EstadoQuery.PENDIENTE;



    // =============== MÉTODOS DE CONVENIENCIA ===============

    /**
     * Verifica si la query necesita ser re-analizada
     */
    public boolean necesitaReanalisis(String hashActual, String versionActual) {
        return this.requiereReanalisis ||
                !this.hashQuery.equals(hashActual) ||
                !this.versionAnalisis.equals(versionActual) ||
                this.estado == EstadoQuery.ERROR;
    }

    /**
     * Actualiza el hash y marca como que necesita re-análisis
     */
    public void actualizarHash(String nuevoHash) {
        if (!this.hashQuery.equals(nuevoHash)) {
            this.hashQuery = nuevoHash;
            this.requiereReanalisis = true;
            this.fechaUltimaActualizacion = LocalDateTime.now();
        }
    }

    /**
     * Marca como registrada manualmente
     */
    public void marcarComoRegistrada(String descripcion) {
        this.esRegistrada = true;
        this.estado = EstadoQuery.REGISTRADA;
        this.descripcion = descripcion;
        this.fechaUltimaActualizacion = LocalDateTime.now();
    }

    /**
     * Obtiene campos de agrupación como lista
     */
    public List<String> getCamposAgrupacionList() {
        return JsonUtils.fromJsonToList(this.camposAgrupacion);
    }

    /**
     * Establece campos de agrupación desde lista
     */
    public void setCamposAgrupacionList(List<String> campos) {
        this.camposAgrupacion = JsonUtils.toJson(campos);
    }

    /**
     * Obtiene campos numéricos como lista
     */
    public List<String> getCamposNumericosList() {
        return JsonUtils.fromJsonToList(this.camposNumericos);
    }

    /**
     * Establece campos numéricos desde lista
     */
    public void setCamposNumericosList(List<String> campos) {
        this.camposNumericos = JsonUtils.toJson(campos);
    }

    /**
     * Obtiene campos de ubicación como lista
     */
    public List<String> getCamposUbicacionList() {
        return JsonUtils.fromJsonToList(this.camposUbicacion);
    }

    /**
     * Establece campos de ubicación desde lista
     */
    public void setCamposUbicacionList(List<String> campos) {
        this.camposUbicacion = JsonUtils.toJson(campos);
    }

    /**
     * Obtiene campos de tiempo como lista
     */
    public List<String> getCamposTiempoList() {
        return JsonUtils.fromJsonToList(this.camposTiempo);
    }

    /**
     * Establece campos de tiempo desde lista
     */
    public void setCamposTiempoList(List<String> campos) {
        this.camposTiempo = JsonUtils.toJson(campos);
    }

    /**
     * Verifica si la query está lista para consolidación
     */
    public boolean estaListaParaConsolidacion() {
        return this.estado == EstadoQuery.ANALIZADA ||
                this.estado == EstadoQuery.REGISTRADA;
    }

    @PrePersist
    protected void onCreate() {
        this.fechaCreacion = LocalDateTime.now();
        this.fechaUltimaActualizacion = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        this.fechaUltimaActualizacion = LocalDateTime.now();
    }
}