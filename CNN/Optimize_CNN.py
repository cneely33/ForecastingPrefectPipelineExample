
########################################################################################################################################################
########################################################## Optimize Model ##########################################################################
########################################################################################################################################################
import pandas as pd
import numpy as np
from gluonts.dataset.split import split
from gluonts.mx.model.seq2seq import MQCNNEstimator
from gluonts.mx import Trainer
from gluonts.evaluation import Evaluator

def dataentry_to_dataframe(entry):
    df = pd.DataFrame(
        entry["target"],
        columns=[entry.get("item_id")],
        index=pd.period_range(
            start=entry["start"], periods=len(entry["target"]), freq=entry["start"].freq
        ),
    )

    return df

class MQCNNTuningObjective:
    def __init__(
                    self, dataset, prediction_length, freq, static_cardinality, metric_type="mean_wQuantileLoss"
                ):
        self.dataset = dataset
        self.prediction_length = prediction_length
        self.freq = freq
        self.metric_type = metric_type
        self.static_cardinality = static_cardinality

        self.train, test_template = split(dataset, offset=-self.prediction_length)
        validation = test_template.generate_instances(
                                                    prediction_length=prediction_length
                                                    )
        self.validation_input = [entry[0] for entry in validation]
        self.validation_label = [
                                dataentry_to_dataframe(entry[1]) for entry in validation
                                ]

    def get_params(self, trial) -> dict:
        return {
            # "num_layers": trial.suggest_int("num_layers", 3, 6),
            # "num_cells": trial.suggest_int("num_cells", 30, 50),
            "learning_rate": trial.suggest_float('learning_rate', 1e-6, 1e-2, log=True),
            "weight_decay": trial.suggest_float('weight_decay', .000005, .001, log=True),
            "context_length": trial.suggest_int("context_length", 3, 9),
            # "context_length": trial.suggest_int("context_length", 9, 15),
            # "distribution": trial.suggest_categorical("dist_option", ['Poisson', 'NegativeBinomal']),
        }

    def __call__(self, trial):
        params = self.get_params(trial)
        
        
        estimator = MQCNNEstimator(
            prediction_length=self.prediction_length,
            context_length= params["context_length"] * self.prediction_length,
            freq=self.freq,
            
            use_feat_static_cat=True,
            use_past_feat_dynamic_real=True,
            use_feat_dynamic_real=True,
            cardinality=self.static_cardinality,
            scaling_decoder_dynamic_feature=True,
            
              # decoder_mlp_dim_seq =[30],
              channels_seq=[30,30,30],
              dilation_seq=[1,3,5],
              kernel_size_seq=[7,3,3],
              quantiles=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
              
            trainer=Trainer(
                ctx="cpu",
                epochs=250,
                learning_rate=params["learning_rate"],
                weight_decay=params["weight_decay"],
                num_batches_per_epoch=100)
          
        )
       
        
        predictor = estimator.train(self.train, cache_data=True)
        forecast_it = predictor.predict(self.validation_input)

        forecasts = list(forecast_it)

        evaluator = Evaluator(quantiles=(np.arange(20) / 20.0)[1:])
        agg_metrics, item_metrics = evaluator(
            self.validation_label, forecasts, num_series=len(self.dataset)
        )
        return agg_metrics[self.metric_type]
    


########################################################################################################################################################
########################################################################################################################################################
