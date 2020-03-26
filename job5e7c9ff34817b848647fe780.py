import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e7c9ff34817b848647fe781','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
	PredictiveHighestIncome_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e7c9ff34817b848647fe781", spark, "{'url': '/Demo/PredictHighestIncomeTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi44999843da7d3a23cf90fd336c0bc37b', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e7c9ff34817b848647fe781','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7c9ff34817b848647fe781','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify','http://23.99.85.149:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e7c9ff34817b848647fe782','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
	PredictiveHighestIncome_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e7c9ff34817b848647fe781"],{"5e7c9ff34817b848647fe781": PredictiveHighestIncome_DBFS}, "5e7c9ff34817b848647fe782", spark,json.dumps( {"FE": [{"transformationsData": {"feature_label": "Occupation"}, "feature": "Occupation", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "60", "mean": "", "stddev": "", "min": "Accountants and auditors", "max": "Word processors and typists", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "M_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "60", "mean": "139.78", "stddev": "211.8", "min": "1", "max": "1111", "missing": "0"}}, {"transformationsData": {"feature_label": "M_weekly"}, "feature": "M_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "60", "mean": "995.36", "stddev": "348.44", "min": "1009", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "F_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "60", "mean": "101.68", "stddev": "179.38", "min": "0", "max": "846", "missing": "0"}}, {"transformationsData": {"feature_label": "F_weekly"}, "feature": "F_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "60", "mean": "813.16", "stddev": "295.89", "min": "1031", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "All_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "60", "mean": "241.52", "stddev": "350.6", "min": "3", "max": "1536", "missing": "0"}}, {"transformationsData": {"feature_label": "All_weekly"}, "feature": "All_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "60", "mean": "913.68", "stddev": "296.12", "min": "1001", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "Occupation_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "60", "mean": "29.5", "stddev": "17.46", "min": "0.0", "max": "59.0", "missing": "0"}}, {"feature": "M_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "60", "mean": "9.35", "stddev": "11.11", "min": "0.0", "max": "33.0", "missing": "0"}}, {"feature": "F_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "60", "mean": "5.42", "stddev": "7.99", "min": "0.0", "max": "25.0", "missing": "0"}}, {"feature": "All_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "60", "mean": "14.35", "stddev": "13.94", "min": "0.0", "max": "41.0", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e7c9ff34817b848647fe782','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7c9ff34817b848647fe782','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify','http://23.99.85.149:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e7c9ff34817b848647fe783','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
	PredictiveHighestIncome_AutoML = tpot_execution.Tpot_execution.run(["5e7c9ff34817b848647fe782"],{"5e7c9ff34817b848647fe782": PredictiveHighestIncome_AutoFE}, "5e7c9ff34817b848647fe783", spark,json.dumps( {"model_type": "classification", "label": "All_weekly", "features": ["Occupation", "M_workers", "M_weekly", "F_workers", "F_weekly", "All_workers"], "percentage": "10", "executionTime": "5", "sampling": "0", "sampling_value": "none", "run_id": "", "ProjectName": "ML Sample Problems", "PipelineName": "PredictHighestIncome", "pipelineId": "5e7c9ff34817b848647fe780", "userid": "5e58ebb7957f3f13254389b5", "runid": "", "url_ResultView": "http://23.99.85.149:3200", "experiment_id": "2341748169460103"}))

	PipelineNotification.PipelineNotification().completed_notification('5e7c9ff34817b848647fe783','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7c9ff34817b848647fe783','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify','http://23.99.85.149:3200/logs/getProductLogs')
	sys.exit(1)

