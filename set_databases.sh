#!/bin/bash

# chmod +x set-databases.sh para darle permisos
# source ./set-databases.sh

export pba_url=jdbc:postgresql://192.168.51.220:5432/saijz
export pba_username=usuario_pba
export pba_password=UlbEXeTpiVKX#

export mda_url=jdbc:postgresql://192.168.50.122:5432/lapampa
export mda_username=usuario_lapampa
export mda_password=nv1VLeexA0ZS#

export santa_rosa_url=jdbc:postgresql://192.168.50.122:5432/avellaneda
export santa_rosa_username=usuario_avellaneda
export santa_rosa_password=ty0y5UQhFCug#

export chaco_url=jdbc:postgresql://192.168.50.122:5432/chaco
export chaco_username=usuario_chaco
export chaco_password=iBqIyh5PPRH7#

export entre_rios_url=jdbc:postgresql://192.168.50.122:5432/entrerios
export entre_rios_username=usuario_entrerios
export entre_rios_password=Z4V7KwE7fByw#

export formosa_url=jdbc:postgresql://192.168.50.122:5432/formosa
export formosa_username=usuario_formosa
export formosa_password=bA5Z2Mc48m#

echo "Variables de entorno de las 6 bases cargadas ✅"