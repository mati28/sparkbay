import java.io.*;
public class FileCopy {
    public static void main(String [] args){

        if ( args.length != 2) {
            System.out.println("Incorrect number of argument");
            return;
        }
        try {
            copy(args[0], args[1]);
        }catch(IOException ioe){
            System.err.println("IO error " + ioe.getMessage());
        }

    }
    static void copy(String source, String destination) throws IOException{
        FileInputStream fis=null;
        FileOutputStream fos = null;
        try{
            fis = new FileInputStream(source);
            fos = new FileOutputStream(destination);
            int c;
            while((c = fis.read()) != -1) {
                fos.write(c);
            }
        }
        finally {
            if (fis != null)
                try {
                    fis.close();
                }catch(IOException ioe){
                    System.err.println(ioe.getMessage());
                }
            if  (fos != null)
                try{
                    fos.close();
                }catch(IOException ioe){
                    System.err.println(ioe.getMessage());
                }

            }       
    }
        
}