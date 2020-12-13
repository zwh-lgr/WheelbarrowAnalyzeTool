import com.google.common.collect.Iterables;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.*;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NonMRUtils {

    public static long nonMRRun(String inputPath,String mergerPath) throws IOException, SQLException, ClassNotFoundException {
        long startTime=System.currentTimeMillis();
        File[] files = getFiles(inputPath);//输入文件夹路径
        int count=0;
        for (File file:files){
            dealOneFile(file,count);//处理每个输入文件
            count++;
        }
        File nonSort=mergeFile(mergerPath);
        sortWithRules(nonSort);
        long runTime=System.currentTimeMillis()-startTime;
        System.out.println(runTime);
        return runTime;
    }

    //得到文件夹下的所有文件
    public static File[] getFiles(String dirPath){
        File directory=new File(dirPath);
        File[] files = directory.listFiles();
        return files;
    }

    //处理单个文件
    public static void dealOneFile(File inFile,int i) throws IOException {
        String line;
        //创建一key多value的map
        MultiValueMap<TextPair,String> map=new LinkedMultiValueMap<>();
        //创建文件输入流
        BufferedReader reader=new BufferedReader(new FileReader(inFile));

        //逐行读取数据并写入到map中，类似于map的功能
        while((line=reader.readLine())!=null){
            if(!line.contains("time,room_id,sender_name")){
                line=line.replace("\"", "");
                String[] splits=line.split(",");

                TextPair textPair=new TextPair();
                textPair.setRoomID(splits[1]);
                textPair.setUserID(splits[2]);
                System.out.println(textPair.getRoomID()+" "+textPair.getUserID());

                map.add(textPair,splits[0]);
            }
        }
        reader.close();

        //创建文件输出流
        String outPath="src/main/resources/wheelanalyzer/output/output"+i+".txt";
        BufferedWriter writer=new BufferedWriter(new FileWriter(new File(outPath)));
        //按key来处理数据，类似于reduce的功能
        for(TextPair key:map.keySet()){
            List<String> valueList = new ArrayList<>();
            List<String> timeList=map.get(key);
            int sum=0;
            for(String time:timeList){
                valueList.add(time);
                sum++;
            }
            if(sum>2){
                // 仅处理发言数大于2的用户
                int count = 0; // 发言数
                long interval = 0; // 本次发言距第一次发言的间隔
                String time;
                // 将发言间隔作为x，发言数作为y
                double[] xData = new double[sum];
                double[] yData = new double[sum];
                // 对发言时间进行排序
                Collections.sort(valueList);
                // 以排序后的第一个时间作为计算原点
                LocalDateTime oX = TimeParser.parseToLocalDateTime(Iterables.getFirst(valueList, null).toString());

                for (String value : valueList) {
                    time = value;
                    interval = Math.abs(Duration.between(oX,TimeParser.parseToLocalDateTime(time)).getSeconds());
                    xData[count] = (double) interval;
                    yData[count] = (double) count + 1;
                    count++;
                }
                double coeffient=new PearsonsCorrelation().correlation(xData, yData);
                //输出到文件中
                String w=key.toString()+"\t"+String.valueOf(sum)+"\t"+String.valueOf(coeffient);
                System.out.println(w);
                writer.write(w);
                writer.write("\n");
                writer.flush();
            }
        }
        writer.close();
    }

    //合并
    public static File mergeFile(String mergePath) throws IOException {
        File directory=new File(mergePath);
        File[] files = directory.listFiles();

        //创建合并的文件和输出流
        File merge=new File(mergePath+"\\merger.txt");
        BufferedWriter writer=new BufferedWriter(new FileWriter(merge,true));

        //合并文件
        for(File file:files){
            BufferedReader reader=new BufferedReader(new FileReader(file));
            String line;
            while((line=reader.readLine())!=null) {
                writer.write(line);
                writer.write("\n");
                writer.flush();
            }
            reader.close();
        }
        return merge;
    }

    //排序
    public static void sortWithRules(File nonSort) throws IOException {
        //创建文件输入流
        BufferedReader reader=new BufferedReader(new FileReader(nonSort));

        ArrayList<outputPair> outputPairsList=new ArrayList<>();
        String line;
        while((line=reader.readLine())!=null){
            String[] splits=line.split("\t");
            outputPair outputPair=new outputPair();
            outputPair.setRoomID(splits[0]);
            outputPair.setUserID(splits[1]);
            outputPair.setSum(Integer.parseInt(splits[2]));
            outputPair.setCoeffient(Double.parseDouble(splits[3]));
            outputPairsList.add(outputPair);

        }
        reader.close();

        //排序
        Collections.sort(outputPairsList);

        //创建文件输出流
        BufferedWriter writer=new BufferedWriter(new FileWriter(nonSort));

        for(outputPair v:outputPairsList){
            writer.write(v.toString());
            writer.write("\n");
            writer.flush();
        }
        writer.close();
    }
}
