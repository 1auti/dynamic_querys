package org.transito_seguro.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class ParametrosFiltrosDTO {

    private String tipoDocumento;
    //private Integer limite, pagina; Esto ya no seria necesario
    private Date fechaInicio, fechaFin, fechaEspecifica;
    private List<String> provincias, municipios, lugares, partido,
            baseDatos, patronesEquipos, tipoVehiculo, filtrarPorTipoEquipo, seriesEquiposExactas;
    private List<Integer> concesiones, tiposInfracciones, estadosInfracciones, tiposDispositivos;
    private Boolean exportadoSacit, tieneEmail, usarTodasLasBDS,
            incluirVLR, incluirSE, incluirTS, soloPersonasJuridicas,consolidacion;

    // Para casos edge
    private Map<String, Object> filtrosAdicionales;


    //------------- CAMPOS DE CURSOR----------------------------------------------------------------

    /**
     * Valor del ultimo registro procesado ( puede ser id , fecha )
     * Se utiliza un String porque soportaria multiples tipos
     */
    private String cursor;

    @Builder.Default
    private String tipoCursor = "fecha_id";

    @Builder.Default
    private Integer pageSize = 50;

    /**
     * Direccion puede "forward" (hacia adelante) & (anterior)
     */
    @Builder.Default
    private String direccion = "forward";  // setteado hacia adelante

    //----------------------- Metodos Privados -----------------------------------------------------


    public boolean esPrimeraPagina() {
        return cursor == null || cursor.trim().isEmpty();
    }

    /**
     * Parsear cursor para fecha id o valor1|valor2|valor3
     */
    public String[] parsearCursor() {
        if (cursor == null || !cursor.contains("|")) {
            return new String[]{cursor};
        }
        return cursor.split("\\|");
    }

    public Long getCursorId() {
        if (cursor == null) return null;

        String[] partes = parsearCursor();
        String ultimaParte = partes[partes.length - 1];

        try {
            return Long.parseLong(ultimaParte);
        } catch (NumberFormatException e) {
            return null; // NO es un ID valido
        }
    }


    /**
     * Obtiene la fecha del cursor (primera parte si es compuesto)
     */
    public String getCursorFecha() {
        if (cursor == null) return null;

        if ("fecha".equals(tipoCursor)) {
            return cursor;
        } else if ("fecha_id".equals(tipoCursor)) {
            String[] partes = parsearCursor();
            return partes.length > 0 ? partes[0] : null;
        }

        return null;
    }

    /**
     * Navegar hacia atrás
     */
    public boolean esHaciaAtras() {
        return "backward".equals(direccion);
    }

    /**
     * Tamaño de página efectivo con límites de seguridad
     */
    public int getPageSizeEfectivo() {
        if (pageSize == null || pageSize <= 0) {
            return 50; // Default
        }
        return Math.min(pageSize, 1000); // Máximo de seguridad
    }

    /**
     * Para queries SQL - siempre +1 para detectar hasNext
     */
    public int getLimiteSql() {
        return getPageSizeEfectivo() + 1;
    }

    // =============== FACTORY METHODS ===============

    /**
     * Crea filtros para primera página
     */
    public static ParametrosFiltrosDTO primeraPagina(int pageSize, String tipoCursor) {
        return ParametrosFiltrosDTO.builder()
                .pageSize(pageSize)
                .tipoCursor(tipoCursor)
                .cursor(null) // Primera página
                .build();
    }

    /**
     * Crea filtros para siguiente página
     */
    public static ParametrosFiltrosDTO siguientePagina(String cursor, String tipoCursor, int pageSize) {
        return ParametrosFiltrosDTO.builder()
                .cursor(cursor)
                .tipoCursor(tipoCursor)
                .pageSize(pageSize)
                .direccion("forward")
                .build();
    }

    /**
     * Crea filtros para página anterior
     */
    public static ParametrosFiltrosDTO paginaAnterior(String cursor, String tipoCursor, int pageSize) {
        return ParametrosFiltrosDTO.builder()
                .cursor(cursor)
                .tipoCursor(tipoCursor)
                .pageSize(pageSize)
                .direccion("backward")
                .build();
    }

    // =============== BUILDER METHODS ===============

    public ParametrosFiltrosDTO conCursor(String cursor) {
        this.cursor = cursor;
        return this;
    }

    public ParametrosFiltrosDTO conTipoCursor(String tipoCursor) {
        this.tipoCursor = tipoCursor;
        return this;
    }

    public ParametrosFiltrosDTO conPageSize(int pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    public ParametrosFiltrosDTO haciaAtras() {
        this.direccion = "backward";
        return this;
    }

    public ParametrosFiltrosDTO haciaAdelante() {
        this.direccion = "forward";
        return this;
    }

    // =============== MÉTODOS DE INFORMACIÓN Y VALIDACIÓN ===============

    public String getInfoPaginacion() {
        return String.format("cursor=%s, tipo=%s, direccion=%s, pageSize=%d",
                cursor != null ? cursor.substring(0, Math.min(cursor.length(), 20)) + "..." : "null",
                tipoCursor, direccion, getPageSizeEfectivo());
    }

    public boolean validarPaginacion() {
        // PageSize válido
        if (pageSize == null || pageSize <= 0 || pageSize > 1000) {
            return false;
        }

        // Tipo de cursor válido
        if (tipoCursor == null ||
                !Arrays.asList("id", "fecha", "fecha_id").contains(tipoCursor)) {
            return false;
        }

        // Dirección válida
        if (direccion == null ||
                !Arrays.asList("forward", "backward").contains(direccion)) {
            return false;
        }

        return true;
    }


    // =============== GENERACIÓN DE CURSOR ===============

    /**
     * Genera cursor desde los valores de un registro
     */
    public static String generarCursor(String tipoCursor, Object... valores) {
        if (valores == null || valores.length == 0) {
            return null;
        }

        switch (tipoCursor) {
            case "id":
                return String.valueOf(valores[0]);

            case "fecha":
                return valores[0].toString(); // Debe ser formato ISO

            case "fecha_id":
                if (valores.length < 2) {
                    throw new IllegalArgumentException("Cursor fecha_id requiere fecha e ID");
                }
                return valores[0] + "|" + valores[1];

            default:
                // Para tipos custom, unir con |
                return String.join("|",
                        java.util.Arrays.stream(valores)
                                .map(String::valueOf)
                                .toArray(String[]::new));
        }
    }

    /**
     * Extrae cursor del último elemento de una lista de resultados
     */
    @SuppressWarnings("unchecked")
    public static String extraerCursorDeResultados(List<Map<String, Object>> resultados, String tipoCursor) {
        if (resultados == null || resultados.isEmpty()) {
            return null;
        }

        Map<String, Object> ultimoRegistro = resultados.get(resultados.size() - 1);

        switch (tipoCursor) {
            case "id":
                Object id = ultimoRegistro.get("id");
                return id != null ? String.valueOf(id) : null;

            case "fecha":
                Object fecha = buscarCampoFecha(ultimoRegistro);
                return fecha != null ? String.valueOf(fecha) : null;

            case "fecha_id":
                Object fechaFI = buscarCampoFecha(ultimoRegistro);
                Object idFI = ultimoRegistro.get("id");
                return (fechaFI != null && idFI != null) ?
                        fechaFI + "|" + idFI : null;

            default:
                // Para custom, usar ID como fallback
                Object fallback = ultimoRegistro.get("id");
                return fallback != null ? String.valueOf(fallback) : null;
        }
    }

    /**
     * Busca campo de fecha en diferentes nombres posibles
     */
    private static Object buscarCampoFecha(Map<String, Object> registro) {
        String[] camposFecha = {"fecha_alta", "fecha", "FECHA_ALTA", "timestamp", "created_at"};

        for (String campo : camposFecha) {
            Object valor = registro.get(campo);
            if (valor != null) {
                return valor;
            }
        }
        return null;
    }

}