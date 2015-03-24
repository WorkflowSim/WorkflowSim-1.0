/**
 * Copyright 2014-2015 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.workflowsim;

import java.util.List;
import org.workflowsim.utils.Parameters.FileType;

/**
 * This is a file implementation in WorkflowSim. Since CloudSim has already
 * implemented File, we call it FileItem here. The reason it is here is
 * WorkflowSim has a different view of files. Case 1: in org.cloudsim.File, file
 * size is integer, while in our case it should be double sine we have many big
 * files. Also, we would like to precisely estimate the transfer delay. Case 2:
 * we would like to specify the type (input, output, intermediate) which is a
 * different concept to the type in CloudSim.
 *
 * @author weiweich
 */
public class FileItem {

    private String name;

    private double size;

    private FileType type;

    public FileItem(String name, double size) {
        this.name = name;
        this.size = size;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSize(double size) {
        this.size = size;
    }

    public void setType(FileType type) {
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public double getSize() {
        return this.size;
    }

    public FileType getType() {
        return this.type;
    }
    
    /**
     * If a input file has an output file it does not need stage-in For
     * workflows, we have a rule that a file is written once and read many
     * times, thus if a file is an output file it means it is generated within
     * this job and then used by another task within the same job (or other jobs
     * maybe) This is useful when we perform horizontal clustering     
     * @param list
     * @return 
     */
    public boolean isRealInputFile(List<FileItem> list) {
        if (this.getType() == FileType.INPUT)//input file
        {
            for (FileItem another : list) {
                if (another.getName().equals(this.getName())
                        /**
                         * if another file is output file
                         */
                        && another.getType() == FileType.OUTPUT) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
