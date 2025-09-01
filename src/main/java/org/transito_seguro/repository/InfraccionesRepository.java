package org.transito_seguro.repository;

import org.transito_seguro.dto.ParametrosFiltrosDTO;

import java.util.List;
import java.util.Map;

public interface InfraccionesRepository {

    List<Map<String,Object>> consultarPersonasJuridicas(ParametrosFiltrosDTO filtro);
}
