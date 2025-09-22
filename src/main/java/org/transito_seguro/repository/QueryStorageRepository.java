package org.transito_seguro.repository;


import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.transito_seguro.enums.EstadoQuery;
import org.transito_seguro.model.QueryStorage;

import java.util.List;
import java.util.Optional;

@Repository
public interface QueryStorageRepository extends JpaRepository<QueryStorage, Long> {

    // Búsqueda por código (identificador único)
    Optional<QueryStorage> findByCodigo(String codigo);

    // Queries activas
    List<QueryStorage> findByActivaTrueOrderByNombreAsc();

    // Queries por categoría
    List<QueryStorage> findByCategoriaAndActivaTrueOrderByNombreAsc(String categoria);

    // Queries consolidables
    List<QueryStorage> findByEsConsolidableAndActivaTrueOrderByNombreAsc(boolean esConsolidable);

    // Queries por estado
    List<QueryStorage> findByEstadoAndActivaTrueOrderByNombreAsc(EstadoQuery estado);

    // Búsqueda por texto (nombre o descripción)
    @Query("SELECT q FROM QueryStorage q WHERE q.activa = true AND " +
            "(LOWER(q.nombre) LIKE LOWER(CONCAT('%', :texto, '%')) OR " +
            "LOWER(q.descripcion) LIKE LOWER(CONCAT('%', :texto, '%'))) " +
            "ORDER BY q.nombre ASC")
    List<QueryStorage> buscarPorTexto(@Param("texto") String texto);

    // Queries más utilizadas
    List<QueryStorage> findByActivaTrueOrderByContadorUsosDescNombreAsc();

    // Queries por tags
    @Query("SELECT q FROM QueryStorage q WHERE q.activa = true AND " +
            "q.tags IS NOT NULL AND " +
            "LOWER(q.tags) LIKE LOWER(CONCAT('%', :tag, '%')) " +
            "ORDER BY q.nombre ASC")
    List<QueryStorage> findByTag(@Param("tag") String tag);

    // Contar queries activas por categoría
    @Query("SELECT q.categoria, COUNT(q) FROM QueryStorage q " +
            "WHERE q.activa = true GROUP BY q.categoria")
    List<Object[]> contarPorCategoria();

    // Queries creadas por usuario
    List<QueryStorage> findByCreadoPorAndActivaTrueOrderByFechaCreacionDesc(String creadoPor);
}

