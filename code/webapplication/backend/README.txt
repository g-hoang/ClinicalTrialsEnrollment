
The backend consists of main file "webserver.py" which runs the pipeline on all data, fits a LGBM and XGB model and provides different API calls.

The following files serve as input for the web service:
- mandatory_transformers: contains all transformers that are applied in the pipeline
- optional_transformers: contains all transformers that can be applied optionally. This depends on the optional hyperparameters identified in hyperopt
- config/requirements.txt: all Python modules necessary for the backend
- config/conf.json: contains general configuration data
- config/lgbm_best_params.json: Optimally determined hyperparameters for LGBM
- config/xgb_best_params.json: Optimally determined hyperparameters for XGB

The following output is generated when running the backend:
- output directory: original and transformed dataframes exported as csv files
- logs directory: log files of preprocessing_service.py and api_service.py

How to start the backend:
- run "python3.6 -W ignore pwebserver.py &"
- wait until the API is ready (check log file)
- access the API using the host's public IP address and the defined port
- Note: make sure the defined port can be reached from the outside. Define a new firewall rule to allow communication over this port. For ubuntu: "sudo ufw allow <PORT>/tcp"

API Calls:
- GET /allvalues
- GET /values?attr=<FEATURE>
- POST /prediction?type=XGB
- POST /prediction?type=LGBM
- GET /sigmoid_img