package org.transito_seguro.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
 public class EstimacionDataset {
        private final int totalEstimado;
        private final double promedioPorProvincia;
        private final int maximoPorProvincia;
}


