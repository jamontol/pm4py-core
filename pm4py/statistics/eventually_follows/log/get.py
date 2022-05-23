'''
    This file is part of PM4Py (More Info: https://pm4py.fit.fraunhofer.de).

    PM4Py is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    PM4Py is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PM4Py.  If not, see <https://www.gnu.org/licenses/>.
'''
from enum import Enum

from pm4py.objects.conversion.log import converter
from pm4py.objects.log.util import sorting
from pm4py.util import exec_utils, constants, xes_constants
from typing import Optional, Dict, Any, Union, Tuple, List, Set
from pm4py.objects.log.obj import EventLog

from pm4py.statistics.concurrent_activities.log import get as conc_act_get

class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_TIMESTAMP_KEY
    START_TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_START_TIMESTAMP_KEY
    KEEP_FIRST_FOLLOWING = "keep_first_following"
    KEEP_CONCURRENT_FOLLOWING = "keep_concurrent_following"

def apply(interval_log: EventLog, parameters: Optional[Dict[Union[str, Parameters], Any]] = None) -> Dict[Tuple[str, str], int]:
    if parameters is None:
        parameters = {}

    interval_log = converter.apply(interval_log, variant=converter.Variants.TO_EVENT_LOG, parameters=parameters)

    concurrent_activities = conc_act_get.apply(interval_log, parameters=parameters)

    activity_key = exec_utils.get_param_value(Parameters.ACTIVITY_KEY, parameters, xes_constants.DEFAULT_NAME_KEY)
    timestamp_key = exec_utils.get_param_value(Parameters.TIMESTAMP_KEY, parameters,
                                               xes_constants.DEFAULT_TIMESTAMP_KEY)
    start_timestamp_key = exec_utils.get_param_value(Parameters.START_TIMESTAMP_KEY, parameters,
                                                     xes_constants.DEFAULT_TIMESTAMP_KEY)
    keep_first_following = exec_utils.get_param_value(Parameters.KEEP_FIRST_FOLLOWING, parameters, False)

    keep_concurrent_following = exec_utils.get_param_value(Parameters.KEEP_CONCURRENT_FOLLOWING, parameters, False)

    ret_dict = {}
    for trace in interval_log:
        sorted_trace = sorting.sort_timestamp_trace(trace, start_timestamp_key)
        i = 0
        while i < len(sorted_trace):
            act1 = sorted_trace[i][activity_key]
            tc1 = sorted_trace[i][timestamp_key]
            j = i + 1

            first = True
            while j < len(sorted_trace):   
                ts2 = sorted_trace[j][start_timestamp_key]
                act2 = sorted_trace[j][activity_key]
                if first:
                    tup_conc = tuple(sorted((act2, act1))) # just to initilize the variable, they won't be concurrent
                else:
                    tup_conc = tuple(sorted((act2, act2_ini)))
                if tc1 <= ts2: 
                    if (keep_concurrent_following and tup_conc in concurrent_activities) or first:
                        tup = (act1, act2)
                        if tup not in ret_dict:
                            ret_dict[tup] = 0
                        ret_dict[tup] = ret_dict[tup] + 1
                     #keep_first_following: 
                        act2_ini = act2
                        first = False 
                    else:
                        break
                    if keep_first_following:
                        break

                j = j + 1
            i = i + 1

    print(ret_dict)
    return ret_dict
