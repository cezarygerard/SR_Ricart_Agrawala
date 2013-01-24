package srprojekt;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class Client {
	public static void main(String[] args) throws InterruptedException,
			IOException {
		HashMap<String, String> params = parseInput(args);
		RAMutex raMutex = new RAMutex(params);
		String useTimeStr = params.get("use_time");
		int useTime;
		int waitTime;
		boolean doRandomTimes = true;
		String doRandomTimesStr = params.get("random");
		if (doRandomTimesStr != null) {
			doRandomTimes = Boolean.parseBoolean(doRandomTimesStr);
		}

		if (useTimeStr != null) {
			useTime = 1000 * Integer.parseInt(useTimeStr);
		} else {
			useTime = 5000;
			doRandomTimes = true;
		}

		String waitTimeStr = params.get("wait_time");
		if (waitTimeStr != null) {
			waitTime = 1000 * Integer.parseInt(waitTimeStr);
		} else {
			waitTime = 5000;
			doRandomTimes = true;
		}

		while (true) {
			Thread.sleep(ThreadLocalRandom.current().nextInt(waitTime));
			raMutex.requestToken();
			doStuff(useTime, doRandomTimes);
			raMutex.releaseToken();
		}
	}

	private static void doStuff(int time, boolean random)
			throws InterruptedException {
		if (random)
			time = ThreadLocalRandom.current().nextInt(time);
		Thread.sleep(ThreadLocalRandom.current().nextInt(time));
		// System.out.println("using...");
	}

	private static HashMap<String, String> parseInput(String[] args)
			throws InvalidParameterException {
		HashMap<String, String> params = new HashMap<String, String>();
		if (args.length % 2 != 0) {
			throw new InvalidParameterException(
					"wrong parameter, should be pairs");
		}

		for (int i = 0; i < args.length; i += 2) {
			params.put(args[i], args[i + 1]);
		}
		return params;
	}
}
