/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.workflowsim.failure;


import org.workflowsim.utils.Parameters;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.cloudbus.cloudsim.Log;

/**
 *
 * @author Weiwei Chen
 */
public  class FailureMonitor {
    
    //protected ClusteringFactor factorClass;
    
    //protected int delay;
    
    /** VM ID 2 Record **/
    protected static Map<Integer, ArrayList<FailureRecord>> vm2record;
    
    /** Type 2 Record**/
    protected static Map<Integer, ArrayList<FailureRecord>> type2record;
    
    /** JobID 2 Record**/
    protected static Map<Integer, FailureRecord> jobid2record;
    
    protected static List<FailureRecord> recordList;

    //protected static String mode;
    
    public static Map index2job;
    
    
    public static void init(/*String _mode*/)
    {   
        vm2record       = new HashMap<Integer, ArrayList<FailureRecord>>();
        type2record     = new HashMap<Integer, ArrayList<FailureRecord>>();
        jobid2record    = new HashMap<Integer, FailureRecord>();
        recordList      = new ArrayList<FailureRecord>();
        //mode            = _mode;
        
    }
    
    protected static double getK(double d, double a, double t)
    {

        if(a<=0.0){
            
        }
        double k = (-d+Math.sqrt(d*d-4*d/Math.log(1-a)))/(2*t);

        return k;
    }

    /** called by Broker**/
    public static int getClusteringFactor( FailureRecord record)
    {
        
        double d  = record.failedTasksNum;//pay attention here
        
        //
        d = record.delayLength;
        
        double t  = record.length ;
        double a  = 0.0;
        switch(Parameters.getMonitorMode()){
            case MONITOR_JOB:
            case MONITOR_ALL:
                a  = analyze( 0, record.depth);
                break;
            case MONITOR_VM:
                a  = analyze (0, record.vmId);
                break;
        }

        if(a<=0.0){
            return record.allTaskNum;
        }else{
            double k  = getK(d, a, t);

            if(k<=1)
                k=1;//minimal

            return (int)k;
        }
    }

    /**called by Broker **/
    public static void postFailureRecord( FailureRecord record)
    {

        if( record.workflowId < 0 || record.jobId < 0 || record.vmId < 0)       
        {
            Log.printLine("Error in receiving failure record");
            return;
        }
        
        switch(Parameters.getMonitorMode()){
            case MONITOR_VM:

                if(!vm2record.containsKey(record.vmId))
                {
                    vm2record.put(record.vmId, new ArrayList<FailureRecord>());
                }
                vm2record.get(record.vmId).add(record);
      
            break;
            case MONITOR_JOB:

                if(!type2record.containsKey(record.depth))
                {
                    type2record.put(record.depth, new ArrayList<FailureRecord>());
                }
                type2record.get(record.depth).add(record);

            break;
            case MONITOR_NONE:
                break;
        }
        
        
//        //for all
//        if(jobid2record.containsKey(record.jobId)){
//            Log.printLine("Something wrong here");
//        }
//        jobid2record.put(record.jobId, record);
        recordList.add(record);
    }
    
    public static double analyze( int workflowId, int type)
    {

        /** workflow level : all jobs together **/
        int sumFailures = 0;
        int sumJobs = 0;
        switch(Parameters.getMonitorMode()){
            case MONITOR_ALL:
        
            for(FailureRecord record:recordList)
            {
                sumFailures += record.failedTasksNum;
                sumJobs += record.allTaskNum;
            }

            break;

            case MONITOR_JOB:
        
            if (type2record.containsKey(type)){
                for(FailureRecord record:type2record.get(type))
                {

                    sumFailures += record.failedTasksNum;
                    sumJobs += record.allTaskNum;
                }
            }

            break;
            case MONITOR_VM:
        
            if (vm2record.containsKey(type)){
                for(FailureRecord record:vm2record.get(type))
                {

                    sumFailures += record.failedTasksNum;
                    sumJobs += record.allTaskNum;
                }
            }

            break;
        }
        
        
        if (sumFailures == 0)
            return 0;
        double alpha = (double)((double)sumFailures/(double)sumJobs);
        return alpha;
        
    }
 

    
}
