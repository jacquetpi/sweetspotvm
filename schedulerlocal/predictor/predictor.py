import vowpalwabbit
import numpy as np
import math, random
from sklearn import datasets
from sklearn.model_selection import train_test_split
from vowpalwabbit.sklearn import (
    VW,
    VWClassifier,
    VWRegressor,
    tovw,
    VWMultiClassifier,
    VWRegressor,
)

class Predictor(object):
    """
    A Predictor is in charge to predict the next active resources
    ...


    Public Methods
    -------
    iterate()
        Deploy a VM to the appropriate CPU subset    
    """
    def __init__(self, **kwargs):
        pass
    
    def predict(self):
        raise ValueError('Not implemented')


class PredictorCsoaa(Predictor):
    """
    This class use a CSOAA: Cost-Sensitive One Against All classifier to predict next active resources
    https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Cost-Sensitive-One-Against-All-%28csoaa%29-multi-class-example
    ...
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        additional_attributes = ['monitoring_window', 'monitoring_learning', 'monitoring_leeway']
        for req_attribute in additional_attributes:
            if req_attribute not in kwargs: raise ValueError('Missing required argument', additional_attributes)
            setattr(self, req_attribute, kwargs[req_attribute])
        self.model_records = dict()
        self.last_features = None
        # Buffer attributes
        self.buffer_timestamp = None
        self.buffer_records = list()
        self.last_prediction = None
        self.last_allocation = 0

        self.output = 'debug/predictor.csv'
        with open(self.output, 'w') as f: f.write('timestamp,prediction,resources,allocation,usage,prev_usage\n')

    def predict(self, timestamp : int, current_resources : int, allocation : int, metric : int):
        # Adapted from SmartHarvest https://dl.acm.org/doi/pdf/10.1145/3447786.3456225
        # Unlike them, we manage a dynamic set of cores (i.e. list of usable resources in our subset )

        if self.buffer_timestamp is None: self.buffer_timestamp = timestamp
        self.buffer_records.append(metric)

        # Tests
        first_call  = False
        safeguard   = False
        buffer_full = False
        if self.last_prediction is None: first_call = True
        if (not first_call) and (current_resources>0) and (math.ceil(metric) >= self.last_prediction): safeguard = True 
        if (not first_call) and ((timestamp - self.buffer_timestamp) >= self.monitoring_learning): buffer_full = True

        delta_allocation = allocation - self.last_allocation
        self.last_allocation = allocation
        self.debug(timestamp=timestamp, current_prediction=self.last_prediction, current_resources=current_resources, allocation=delta_allocation, current_usage=metric)

        if first_call:
            self.last_prediction = current_resources
            return current_resources

        if (not safeguard) and (not buffer_full):
            return self.last_prediction

        if safeguard:
            print('##Safeguard')
            prediction = math.ceil(self.last_prediction+5)
            if prediction>current_resources: prediction=current_resources
            self.last_prediction = prediction
            return prediction
        else:
            prediction = self.predict_on_new_model(timestamp=timestamp, current_resources=current_resources, metrics=self.buffer_records)
            prediction = math.ceil(prediction+8)
            if prediction>current_resources: prediction=current_resources

            self.buffer_timestamp = None
            self.buffer_records = list()
            self.last_prediction = prediction
            return prediction

    def predict_on_new_model(self, timestamp : int, current_resources : int, metrics : list):
        # Adapted from SmartHarvest https://dl.acm.org/doi/pdf/10.1145/3447786.3456225
        # Unlike them, we manage a dynamic set of cores (i.e. list of usable resources in our subset )
        
        # First, register peak associated to last iteration features
        if self.last_features is not None:
            self.add_record(timestamp=timestamp, peak_usage=max(metrics), features=self.last_features)

        # Generate current features
        current_features = self.__generate_features(metrics=metrics)
        self.last_features = current_features

        # Safeguard on empty subsets and models without data
        if current_resources<=0 or not self.contains_enough_data():
            return current_resources
        
        # Second, update model
        vw = vowpalwabbit.Workspace(csoaa=current_resources, quiet=True)
        raw_data = self.__generate_data_from_records(resources_count=current_resources)
        random.shuffle(raw_data) 
        for data in raw_data: 
            vw.learn(data)

        # Third, predict next peak based on model
        prediction = vw.predict('| ' + current_features)
        vw.finish()
        return prediction + np.std(metrics)

    def debug(self, timestamp : int, current_prediction : int, current_resources : int, allocation : float, current_usage : float):
        if not hasattr(self, 'prev_usage'): self.prev_usage = None
        with open(self.output, 'a') as f: 
            line = str(timestamp)  + ',' + str(current_prediction) + ',' + str(current_resources) + ',' + str(allocation) + ',' + str(current_usage) + ',' + str(self.prev_usage)
            f.write(line + '\n')
        self.prev_usage = current_usage

    def __generate_data_from_records(self, resources_count : int):
        data = list()
        for record_tuple in self.model_records.values():
            (peak_usage, features) = record_tuple
            label = self.__generate_labels_with_costs(resources_count=resources_count, observed_peak=peak_usage)
            data.append(label + ' | ' + features)
        return data

    def __generate_labels_with_costs(self, resources_count : int, observed_peak : float):
        costs = ''
        actual_peak_rounded = math.ceil(observed_peak)
        negative_penalty_start = (resources_count - actual_peak_rounded)
        for core in range(1, resources_count+1):
            delta = np.abs(core-actual_peak_rounded)
            associated_cost = negative_penalty_start + delta if (core < actual_peak_rounded) else delta
            costs+= str(core) + ':' + str(float(associated_cost)) + ' '
        return costs[:-1]

    def __generate_features(self, metrics : list):
        """Get CSOAA features as a string
        ----------

        Parameters
        ----------
        metrics : list
            List of usage resources to use to generate the feature

        Returns
        -------
        Features : str
            Features as string
        """
        return 'min:' + str(round(min(metrics),3)) + ' max:' + str(round(max(metrics),3)) +\
            ' avg:' + str(round(np.mean(metrics),3)) +  ' std:' + str(round(np.std(metrics),3)) + ' med:' + str(round(np.median(metrics),3))

    def add_record(self, timestamp : int, peak_usage : float, features : str):
        """Add new records to the collection attributes and manage expired data
        ----------

        Parameters
        ----------
        timestamp : int
            The timestamp key
        peak_usage : float
            Peak observed in this timestamp identified window
        features : str
            The last generated features
        """
        self.model_records[timestamp] = (peak_usage, features)
        self.remove_expired_keys(timestamp=timestamp, considered_dict=self.model_records)

    def remove_expired_keys(self, timestamp : int, considered_dict : dict):
        """Parse a dict where the key is a timestamp and remove all values being older than
        timestamp - self.MONITORING_WINDOW
        ----------

        Parameters
        ----------
        timestamp : int
            The timestamp key
        considered_dict : dict
            Dict to filter
        """
        records_to_remove = list()
        for record_timestamp in considered_dict.keys():
            if record_timestamp < (timestamp - self.monitoring_window): records_to_remove.append(record_timestamp)
        for record_to_remove in records_to_remove: del considered_dict[record_to_remove]

    def contains_enough_data(self):
        """Check if self.model_records contains enough historical records based on requirements
        ----------

        Return
        ----------
        result : bool
            True/False
        """
        if not self.model_records: return False # Empty dict
        covered_period = max(self.model_records.keys()) - min(self.model_records.keys())
        return covered_period >= (self.monitoring_window - self.monitoring_learning*2) # *2 to manage threshold effect on lower/upper bound

class PredictorMaxVMPeak(Predictor):

    def predict(self):
        #TODO: to fix
        res_needed_count = 0
        threshold_cpu    = 0
        for consumer in self.consumer_list:
            if threshold_cpu < consumer.get_cpu(): threshold_cpu = consumer.get_cpu() 
            if (consumer.get_uuid() not in self.hist_consumers_usage or len(self.hist_consumers_usage[consumer.get_uuid()]) < self.MONITORING_MIN):
                res_needed_count+= consumer.get_cpu() # not enough data
            # else:
            #     consumer_records  = [value for __, value in self.hist_consumers_usage[consumer.get_uuid()]]
            #     consumer_max_peak = consumer.get_cpu() * max(consumer_records) + self.MONITORING_LEEWAY*np.std(consumer_records)
            #     if consumer.get_cpu() < consumer_max_peak: consumer_max_peak = consumer.get_cpu()
            
            # res_needed_count += consumer_max_peak

        # Compute next peak
        subset_records  = [value for __, value in self.hist_usage]
        usage_current   = subset_records[-1] if subset_records else None
        usage_predicted = math.ceil(max(subset_records) + self.MONITORING_LEEWAY*np.std(subset_records)) if len(subset_records) >= self.MONITORING_MIN else len(self.get_res())

        # Watchdog, was our last prediction too pessimistic?
        if usage_current is not None and (math.ceil(usage_current) == len(self.active_res)): 
            print('#DEBUG: watchdog')
            usage_predicted = len(self.get_res())
        else:
            print('#DEBUG: no-watchdog')
        # Watchdog, do not overcommit a VM with itself
        if usage_predicted < threshold_cpu: usage_predicted = threshold_cpu
        # Watchdog, is there new VMs?
        if usage_predicted < res_needed_count: usage_predicted = res_needed_count

if __name__ == '__main__':
    # Test environment, to be removed
    num_classes = 23
    vw = vowpalwabbit.Workspace('--csoaa 23', quiet=False)
    raw_data = [
        "1:0.0 2:1.0 3:1.0 4:1.0 | a:0 b:1 c:1",
        "1:2.0 2:0.0 3:2.0 4:2.0 | b:1 c:1 d:1",
        "1:0.0 2:1.0 3:1.0 4:1.0 | a:8 c:1 e:1",
        "1:1.0 2:1.0 3:1.0 4:0.0 | b:1 d:1 f:1",
        "1:1.0 2:2.0 3:0.0 4:1.0 | d:1 e:1 f:1"
    ]
    raw_data = ['1:22.0 2:21.0 3:20.0 4:0.0 5:1.0 6:2.0 7:3.0 8:4.0 9:5.0 10:6.0 11:7.0 12:8.0 13:9.0 14:10.0 15:11.0 16:12.0 17:13.0 18:14.0 19:15.0 20:16.0 21:17.0 22:18.0 23:19.0 | min:2.0 max:2.6 avg:2.3 std:0.2 med:2.4', '1:22.0 2:21.0 3:20.0 4:19.0 5:0.0 6:1.0 7:2.0 8:3.0 9:4.0 10:5.0 11:6.0 12:7.0 13:8.0 14:9.0 15:10.0 16:11.0 17:12.0 18:13.0 19:14.0 20:15.0 21:16.0 22:17.0 23:18.0 | min:1.8 max:3.8 avg:2.6 std:0.7 med:2.4', '1:22.0 2:21.0 3:20.0 4:19.0 5:18.0 6:17.0 7:16.0 8:0.0 9:1.0 10:2.0 11:3.0 12:4.0 13:5.0 14:6.0 15:7.0 16:8.0 17:9.0 18:10.0 19:11.0 20:12.0 21:13.0 22:14.0 23:15.0 | min:2.9 max:4.1 avg:3.5 std:0.3 med:3.4', '1:22.0 2:21.0 3:20.0 4:19.0 5:18.0 6:17.0 7:0.0 8:1.0 9:2.0 10:3.0 11:4.0 12:5.0 13:6.0 14:7.0 15:8.0 16:9.0 17:10.0 18:11.0 19:12.0 20:13.0 21:14.0 22:15.0 23:16.0 | min:3.4 max:7.3 avg:5.4 std:0.8 med:5.4', '1:22.0 2:21.0 3:20.0 4:19.0 5:18.0 6:17.0 7:16.0 8:0.0 9:1.0 10:2.0 11:3.0 12:4.0 13:5.0 14:6.0 15:7.0 16:8.0 17:9.0 18:10.0 19:11.0 20:12.0 21:13.0 22:14.0 23:15.0 | min:4.7 max:6.6 avg:5.7 std:0.6 med:5.8', '1:22.0 2:21.0 3:20.0 4:19.0 5:18.0 6:17.0 7:16.0 8:15.0 9:14.0 10:0.0 11:1.0 12:2.0 13:3.0 14:4.0 15:5.0 16:6.0 17:7.0 18:8.0 19:9.0 20:10.0 21:11.0 22:12.0 23:13.0 | min:4.4 max:7.1 avg:5.7 std:0.7 med:5.7', '1:22.0 2:21.0 3:20.0 4:19.0 5:18.0 6:17.0 7:16.0 8:15.0 9:14.0 10:13.0 11:12.0 12:0.0 13:1.0 14:2.0 15:3.0 16:4.0 17:5.0 18:6.0 19:7.0 20:8.0 21:9.0 22:10.0 23:11.0 | min:5.9 max:9.7 avg:7.2 std:0.9 med:6.9', '1:22.0 2:21.0 3:20.0 4:19.0 5:18.0 6:17.0 7:16.0 8:15.0 9:14.0 10:13.0 11:0.0 12:1.0 13:2.0 14:3.0 15:4.0 16:5.0 17:6.0 18:7.0 19:8.0 20:9.0 21:10.0 22:11.0 23:12.0 | min:7.9 max:11.4 avg:9.0 std:0.8 med:9.0', '1:22.0 2:21.0 3:20.0 4:19.0 5:18.0 6:17.0 7:16.0 8:15.0 9:14.0 10:13.0 11:12.0 12:11.0 13:0.0 14:1.0 15:2.0 16:3.0 17:4.0 18:5.0 19:6.0 20:7.0 21:8.0 22:9.0 23:10.0 | min:8.8 max:10.2 avg:9.4 std:0.3 med:9.4', '1:22.0 2:21.0 3:20.0 4:19.0 5:18.0 6:17.0 7:16.0 8:15.0 9:14.0 10:13.0 11:12.0 12:11.0 13:0.0 14:1.0 15:2.0 16:3.0 17:4.0 18:5.0 19:6.0 20:7.0 21:8.0 22:9.0 23:10.0 | min:9.2 max:12.4 avg:11.0 std:0.7 med:11.2', '1:22.0 2:21.0 3:20.0 4:19.0 5:18.0 6:17.0 7:16.0 8:15.0 9:14.0 10:13.0 11:12.0 12:11.0 13:10.0 14:0.0 15:1.0 16:2.0 17:3.0 18:4.0 19:5.0 20:6.0 21:7.0 22:8.0 23:9.0 | min:10.1 max:12.2 avg:11.0 std:0.6 med:10.9', '1:22.0 2:21.0 3:20.0 4:19.0 5:18.0 6:17.0 7:16.0 8:15.0 9:14.0 10:13.0 11:12.0 12:11.0 13:10.0 14:9.0 15:8.0 16:0.0 17:1.0 18:2.0 19:3.0 20:4.0 21:5.0 22:6.0 23:7.0 | min:11.7 max:13.3 avg:12.6 std:0.5 med:12.5']
    random.shuffle(raw_data)
    for data in raw_data: 
        print(data)
        vw.learn(data)
    prediction = vw.predict('| min:12.65750209843019 max:13.346548780257843 avg:12.571838189726865 std:0.45199516092884257 med:12.468942239055583')
    print(prediction)
    print('test', len(raw_data))
    vw.finish()