#!/bin/bash
# chmod +x set-databases.sh para darle permisos
# source ./set-databases.sh

export PBA_URL=jdbc:postgresql://192.168.51.220:5432/pba
export PBA_USERNAME=usuario_pba
export PBA_PASSWORD=UlbEXeTpiVKX#

export MDA_URL=jdbc:postgresql://192.168.50.122:5432/lapampa
export MDA_USERNAME=usuario_lapampa
export MDA_PASSWORD=nv1VLeexA0ZS#

export SANTA_ROSA_URL=jdbc:postgresql://192.168.50.122:5432/avellaneda
export SANTA_ROSA_USERNAME=usuario_avellaneda
export SANTA_ROSA_PASSWORD=ty0y5UQhFCug#

export CHACO_URL=jdbc:postgresql://192.168.50.122:5432/chaco
export CHACO_USERNAME=usuario_chaco
export CHACO_PASSWORD=iBqIyh5PPRH7#

export ENTRE_RIOS_URL=jdbc:postgresql://192.168.50.122:5432/entrerios
export ENTRE_RIOS_USERNAME=usuario_entrerios
export ENTRE_RIOS_PASSWORD=Z4V7KwE7fByw#

export FORMOSA_URL=jdbc:postgresql://192.168.50.122:5432/formosa
export FORMOSA_USERNAME=usuario_formosa
export FORMOSA_PASSWORD=bA5Z2Mc48m#

echo "Variables de entorno de las 6 bases cargadas ✅"