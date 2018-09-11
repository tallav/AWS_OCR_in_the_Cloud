package Manager;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ManagerRecieveTask implements Runnable {
   private int counter;
   private int totalMsgAmount;
   private SQSClass resultsSQS;
   private HashMap mapper;

   public ManagerRecieveTask(SQSClass resultsSQS,  int totalMsgAmount){
       this.resultsSQS = resultsSQS;
       mapper = new HashMap();
       this.totalMsgAmount = totalMsgAmount;
   }

    public void run() {
        Message msg = null;
        counter = 0;
        while (totalMsgAmount>0) {
            msg = resultsSQS.receiveMessage();
            System.out.println("recieve thread is waiting!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            if (msg != null) {
                try {
                    totalMsgAmount--;
                    mapper.put(counter,((TextMessage) msg).getText());
                    counter++;
                    System.out.println("msg added to the mapper, total msg amount: "+ totalMsgAmount);
                    msg.acknowledge();
                } catch (JMSException e) {
                	e.printStackTrace();
                }
        	}
        }
    }

    protected HashMap getMappedResults(){
        return mapper;
    }

    private List<String> parseMsg(String msgToParse){
        int index = msgToParse.indexOf("Result: ");
        String url = msgToParse.substring(0,index);
        String res = msgToParse.substring(index+8);
        List<String> result = new ArrayList<String>();
        result.add(url);
        result.add(res);
        return result;
    }

}