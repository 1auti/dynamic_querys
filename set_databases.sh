#!/bin/bash

# chmod +x set-databases.sh para darle permisos
# source .set/-databases.sh

export DB1_URL=jdbc:postgresql://192.168.51.220:5432/saijz
export DB1_USER=usuario_pba
export DB1_PASS=UlbEXeTpiVKX#

export DB2_URL= jdbc:postgresql://192.168.50.122:5432/lapampa
export DB2_USER=usuario_lapampa
export DB2_PASS=nv1VLeexA0ZS#

export DB3_URL=jdbc:postgresql://192.168.50.122:5432/avellaneda
export DB3_USER=usuario_avellaneda
export DB3_PASS=ty0y5UQhFCug#

export DB4_URL=jdbc:postgresql://192.168.50.122:5432/chaco
export DB4_USER=usuario_chaco
export DB4_PASS=iBqIyh5PPRH7#

export DB5_URL=jdbc:postgresql://192.168.50.122:5432/entrerios
export DB5_USER=usuario_entrerios
export DB5_PASS=Z4V7KwE7fByw#

export DB6_URL=jdbc:postgresql://192.168.50.122:5432/formosa
export DB6_USER=usuario_formosa
export DB6_PASS=bA5Z2Mc48m#

echo "Variables de entorno de las 6 bases cargadas ✅"

