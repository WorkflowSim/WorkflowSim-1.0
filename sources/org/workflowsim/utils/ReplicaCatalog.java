/*
 * 
 *   Copyright 2007-2008 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */
package org.workflowsim.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Weiwei Chen
 */
public class ReplicaCatalog {
        /*
     * File System
     */
    public enum FileSystem {
        SHARED, LOCAL
    }
    private static Map FileName2File;
    private static FileSystem fileSystem;
    private static Map dataReplicaCatalog;
    public static void init(FileSystem fs){
        fileSystem = fs;
        dataReplicaCatalog = new HashMap< String, List>();
        FileName2File      = new HashMap<String, org.cloudbus.cloudsim.File>();
    }
    
    public static FileSystem getFileSystem(){
        return fileSystem;
    }
    public static org.cloudbus.cloudsim.File getFile(String fileName){
        return (org.cloudbus.cloudsim.File)FileName2File.get(fileName);
    }
    public static void setFile(String fileName, org.cloudbus.cloudsim.File file){
        FileName2File.put(fileName, file);
    }
    public static boolean containsFile(String fileName){
        return FileName2File.containsKey(fileName);
    }

    public static List getStorageList(String file){
        return (List) dataReplicaCatalog.get(file);
    }
    /*
     * it is ok if you have duplicate
     */
    public static void addStorageList(String file, String storage){
        if(!dataReplicaCatalog.containsKey(file)){
            dataReplicaCatalog.put(file, new ArrayList());
        }
        List list = getStorageList(file);
        if(!list.contains(storage)){
            list.add(storage);
        }
    }

}
