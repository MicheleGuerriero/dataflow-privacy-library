package example1.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Collections;

import org.apache.flink.api.java.tuple.Tuple2;

public class StreamMerger {

	public static void mergeVcp(String experimentFolderPath) throws FileNotFoundException {

		sortStream(new File(experimentFolderPath + "/s2.txt"), new File(experimentFolderPath + "/s2_sorted.txt"));
		sortStream(new File(experimentFolderPath + "/s3.txt"), new File(experimentFolderPath + "/s3_sorted.txt"));
		sortStream(new File(experimentFolderPath + "/s4.txt"), new File(experimentFolderPath + "/s4_sorted.txt"));
		sortStream(new File(experimentFolderPath + "/s5.txt"), new File(experimentFolderPath + "/s5_sorted.txt"));
		sortStream(new File(experimentFolderPath + "/s1_p.txt"), new File(experimentFolderPath + "/s1_p_sorted.txt"));
		
		merge(new File(experimentFolderPath + "/s1_p_sorted.txt"), new File(experimentFolderPath + "/s2_sorted.txt"),
				new File(experimentFolderPath + "/merged-tmp-private.txt"));
		merge(new File(experimentFolderPath + "/s1.txt"), new File(experimentFolderPath + "/s2_sorted.txt"),
				new File(experimentFolderPath + "/merged-tmp.txt"));
		
		merge(new File(experimentFolderPath + "/s3_sorted.txt"), new File(experimentFolderPath + "/merged-tmp.txt"),
				new File(experimentFolderPath + "/merged-tmp2.txt"));
		merge(new File(experimentFolderPath + "/s3_sorted.txt"), new File(experimentFolderPath + "/merged-tmp-private.txt"),
				new File(experimentFolderPath + "/merged-tmp-private2.txt"));
		
		merge(new File(experimentFolderPath + "/s4_sorted.txt"), new File(experimentFolderPath + "/merged-tmp2.txt"),
				new File(experimentFolderPath + "/merged-tmp3.txt"));
		merge(new File(experimentFolderPath + "/s4_sorted.txt"), new File(experimentFolderPath + "/merged-tmp-private2.txt"),
				new File(experimentFolderPath + "/merged-tmp-private3.txt"));
		
		merge(new File(experimentFolderPath + "/s5_sorted.txt"), new File(experimentFolderPath + "/merged-tmp3.txt"),
				new File(experimentFolderPath + "/merged-tmp4.txt"));
		merge(new File(experimentFolderPath + "/s5_sorted.txt"), new File(experimentFolderPath + "/merged-tmp-private3.txt"),
				new File(experimentFolderPath + "/merged-tmp-private4.txt"));
		
		merge(new File(experimentFolderPath + "/ctx.txt"), new File(experimentFolderPath + "/merged-tmp-private4.txt"),
				new File(experimentFolderPath + "/merged-private.log"));
		merge(new File(experimentFolderPath + "/ctx.txt"), new File(experimentFolderPath + "/merged-tmp4.txt"),
				new File(experimentFolderPath + "/merged.log"));

	}
	
	public static void mergeDsep(String experimentFolderPath) throws FileNotFoundException {
		sortStream(new File(experimentFolderPath + "/s2.txt"), new File(experimentFolderPath + "/s2_sorted.txt"));
		sortStream(new File(experimentFolderPath + "/s3.txt"), new File(experimentFolderPath + "/s3_sorted.txt"));
		sortStream(new File(experimentFolderPath + "/s4.txt"), new File(experimentFolderPath + "/s4_sorted.txt"));
		sortStream(new File(experimentFolderPath + "/s5.txt"), new File(experimentFolderPath + "/s5_sorted.txt"));
		
		
		merge(new File(experimentFolderPath + "/s3_sorted.txt"), new File(experimentFolderPath + "/s2_sorted.txt"),
				new File(experimentFolderPath + "/merged-tmp.txt"));
		
		merge(new File(experimentFolderPath + "/s4_sorted.txt"), new File(experimentFolderPath + "/merged-tmp.txt"),
				new File(experimentFolderPath + "/merged-tmp2.txt"));
		
		merge(new File(experimentFolderPath + "/s5_sorted.txt"), new File(experimentFolderPath + "/merged-tmp2.txt"),
				new File(experimentFolderPath + "/merged-tmp3.txt"));
		
		merge(new File(experimentFolderPath + "/ctx.txt"), new File(experimentFolderPath + "/merged-tmp3.txt"),
				new File(experimentFolderPath + "/merged-private.log"));
	}
	
	public static void genericMerge(String experimentFolderPath, List<String> streamNames, String output) {
		
		
		int count = 0;
		String previous = null;
		
		for(String s: streamNames) {
			sortStream(new File(experimentFolderPath + "/" + s +".txt"), new File(experimentFolderPath + "/" + s + "_sorted.txt"));
		}
		
		Iterator<String> iter = streamNames.iterator();
		if(iter.hasNext()) {
			previous = iter.next();
			merge(new File(experimentFolderPath + "/" + previous + "_sorted.txt"), new File(experimentFolderPath + "/" + iter.next() + "_sorted.txt"),
					new File(experimentFolderPath + "/merged-tmp" + count + ".log"));
		} else {
			throw new IllegalStateException("At least one stream should be provided.");
		}
		
		while(iter.hasNext()) {
			String current = iter.next();
			if(iter.hasNext()) {
			merge(new File(experimentFolderPath + "/merged-tmp" + count + ".log"), new File(experimentFolderPath + "/" + current + "_sorted.txt"),
					new File(experimentFolderPath + "/merged-tmp" + (count + 1) + ".log"));
			count = count + 1;
			} else {
				merge(new File(experimentFolderPath + "/merged-tmp" + count + ".log"), new File(experimentFolderPath + "/" + current + "_sorted.txt"),
						new File(output));
			}
		}
	}

	private static void sortStream(File input, File output) {

		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(input));
			BufferedWriter out = new BufferedWriter(new FileWriter(output));

			List<Tuple2<Long, String>> stream = new ArrayList<Tuple2<Long, String>>();

			String line = in.readLine();

			while (line != null) {

				stream.add(new Tuple2<Long, String>(
						Long.parseLong(line.split(" ")[0].substring(1, line.split(" ")[0].length())), line));
				line = in.readLine();
			}

			Collections.sort(stream, new Comparator<Tuple2<Long, String>>() {
				@Override
				public int compare(Tuple2<Long, String> a, Tuple2<Long, String> b) {
					return a.f0 < b.f0 ? -1 : a.f0.equals(b.f0) ? 0 : 1;
				}
			});

			for (Tuple2<Long, String> e : stream) {
				out.write(e.f1 + "\n");
			}
			in.close();
			out.close();
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	public static void merge(File file1, File file2, File out) {

		try {
			BufferedReader br1 = new BufferedReader(new FileReader(file1));
			BufferedReader br2 = new BufferedReader(new FileReader(file2));
			PrintWriter pw = new PrintWriter(new FileWriter(out));

			String line1 = br1.readLine();
			String line2 = br2.readLine();

			while (line1 != null || line2 != null) {
				if (line1 != null && line2 != null) {
					Long t1 = Long.parseLong(line1.split(" ")[0].substring(1));
					Long t2 = Long.parseLong(line2.split(" ")[0].substring(1));
					if (t2 >= t1) {
						pw.write(line1 + "\n");
						line1 = br1.readLine();
					} else {
						pw.write(line2 + "\n");
						line2 = br2.readLine();
					}
				} else if (line1 != null && line2 == null) {
					pw.write(line1 + "\n");
					line1 = br1.readLine();
				} else if (line1 == null && line2 != null) {
					pw.write(line2 + "\n");
					line2 = br2.readLine();
				}
			}
			pw.close();
			br1.close();
			br2.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
