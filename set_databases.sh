#!/bin/bash
# chmod +x set-databases.sh para darle permisos
# source ./set-databases.sh


export PBA_URL=jdbc:postgresql://192.168.50.122:5432/pba
export PBA_USERNAME=lcenizo
export PBA_PASSWORD=Lcenizo2025#

export MDA_URL=jdbc:postgresql://192.168.50.122:5432/lapampa
export MDA_USERNAME=lcenizo
export MDA_PASSWORD=Lcenizo2025#

export SANTA_ROSA_URL=jdbc:postgresql://192.168.50.122:5432/avellaneda
export SANTA_ROSA_USERNAME=lcenizo
export SANTA_ROSA_PASSWORD=Lcenizo2025#

export CHACO_URL=jdbc:postgresql://192.168.50.122:5432/chaco
export CHACO_USERNAME=lcenizo
export CHACO_PASSWORD=Lcenizo2025#

export ENTRE_RIOS_URL=jdbc:postgresql://192.168.50.122:5432/entrerios
export ENTRE_RIOS_USERNAME=lcenizo
export ENTRE_RIOS_PASSWORD=Lcenizo2025#

export FORMOSA_URL=jdbc:postgresql://192.168.50.122:5432/formosa
export FORMOSA_USERNAME=lcenizo
export FORMOSA_PASSWORD=Lcenizo2025#

echo "Variables de entorno de las 6 bases cargadas ✅"
