package org.transito_seguro.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.transito_seguro.enums.EstadoQuery;
import org.transito_seguro.model.QueryMetadata;

import java.util.List;
import java.util.Optional;

@Repository
public interface QueryMetadataRepository extends JpaRepository<QueryMetadata,Long> {

    Optional<QueryMetadata> findByNombreQuery(String nombreQuery);

    List<QueryMetadata> findByEsConsolidable(boolean esConsolidable);

    List<QueryMetadata> findByRequiereReanalisis(boolean requiereReanalisis);

    List<QueryMetadata> findByEstado(EstadoQuery estado);

}
