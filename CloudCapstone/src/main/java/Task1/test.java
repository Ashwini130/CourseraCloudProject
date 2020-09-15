package Task1;

public class test {
	public static void main(String args[]) {
		
		String data[] = new String[] {"Ashwini","Shivaji","Patil"};
		MultiWritable checkdata = new MultiWritable();
		System.out.println(data[2]);
		checkdata.setData(data);
		System.out.println(checkdata);
		
		
		
	}
}
