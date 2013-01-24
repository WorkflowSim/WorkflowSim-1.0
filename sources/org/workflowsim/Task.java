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
package org.workflowsim;

import java.util.ArrayList;
import java.util.List;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;

/**
 *
 * @author Weiwei Chen
 */
public class Task extends Cloudlet{
    

    private List<Task>parentList;
    private List<Task>childList;
    private List<org.cloudbus.cloudsim.File>fileList;
    private int priority;
    private int depth;
    private double impact;
    //replace existing input file size and output file size
    //private long inputDataSize;
    
    //private long outputDataSize;
    //only valid for task level
    //optional
    private String type;
    
//    public void setInputDataSize(long size)
//    {
//        this.inputDataSize = size;
//    }
//    public void setOutputDataSize(long size)
//    {
//        this.outputDataSize = size;
//    }
    
    public Task(
                    final int cloudletId,
                    final long cloudletLength/*,
                    final long cloudletFileSize,
                    final long cloudletOutputSize*/
                ) 
    {

        super(cloudletId, cloudletLength, 1, 0, 0, /*, cloudletFileSize, cloudletOutputSize,*/ new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());


        this.childList      = new ArrayList<Task>();
        this.parentList     = new ArrayList<Task>();
        this.fileList       = new ArrayList<org.cloudbus.cloudsim.File>();
        this.impact         = 0.0;
    }

    
    public void setType(String type){
        this.type = type;
    }
    public String getType(){
        return type;
    }
    public void setPriority(int priority)
    {
        this.priority = priority;
    }
    public void setDepth(int depth){
        this.depth = depth;
    }
    public int getPriority(){
        
        return this.priority;
    }
    public int getDepth(){
        return this.depth;
    }
    
    //Operators for childlist and parentlist
    public List<Task> getChildList()
    {
        return this.childList;
    }
    
    public void setChildList(List list){
        this.childList = list;
    }
    public void setParentList(List list){
        this.parentList = list;
    }
    public void addChildList(List list){
        this.childList.addAll(list);
    }
    public void addParentList(List list){
        this.parentList.addAll(list);
    }
    public List<Task> getParentList()
    {
        return this.parentList;
    }
    public void addChild(Task task){
        this.childList.add(task);
    }
    public void addParent(Task task){
        this.parentList.add(task);
    }
    
    public List getFileList(){
        return this.fileList;
    }
    public void addFile(org.cloudbus.cloudsim.File file){
        this.fileList.add(file);
    }
    public void setFileList(List<org.cloudbus.cloudsim.File> list)
    {
        this.fileList = list;
    }
    
    public void setImpact(double impact){
        this.impact = impact;
    }
    public double getImpact(){
        return this.impact;
    }
}
