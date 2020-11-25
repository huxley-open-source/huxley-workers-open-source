package com.thehuxley.workers;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Test;

import com.thehuxley.data.Configurator;
import com.thehuxley.data.DataManager;
import com.thehuxley.data.JsonUtils;
import com.thehuxley.data.model.Submission;

public class EvaluationWorkerTest {

	@Test
	public void testOnMessage() throws IOException, InterruptedException {
		int counter = 0;
		int loops = 3;
		String fileName = Configurator.getProperties(Constants.CONF_FILENAME)
                .getProperty("worker.evaluation.temp.dir") + Configurator.getProperties(Constants.CONF_FILENAME)
				.getProperty("worker.evaluation.queue_ids_filename")+"_"+1;
		int shortTime = Integer.parseInt(Configurator.getProperties(
				Constants.CONF_FILENAME).getProperty(
				"worker.evaluation.short_time"));
		EvaluationWorker worker = new EvaluationWorker(1);
		File f = new File(fileName);
		f.delete();

		Submission s = null;
		int id = 0;
		for (int i = 0; i < loops; i++) {
			while (s == null) {
				id++;
				s = DataManager.getSubmissionById(id);
			}
			String jsonMsg = JsonUtils.toJson(s);
			worker.onMessage(jsonMsg);
			// manda de novo
			worker.onMessage(jsonMsg);
			s = null;
		}

		BufferedReader br = new BufferedReader(new FileReader(fileName));
		while (br.readLine() != null) {
			counter++;
		}
		assertTrue(counter > 0);
		br.close();

		Thread.sleep(shortTime + 1000);

		// manda mais uma mensagem
		while (s == null) {
			id++;
			s = DataManager.getSubmissionById(id);
		}
		String jsonMsg = JsonUtils.toJson(s);
		worker.onMessage(jsonMsg);

		try {
			br = new BufferedReader(new FileReader(fileName));
			fail("The file should not exist because the messages should have been sent.");
		} catch (FileNotFoundException e) {
			assertTrue(true);
		}

	}

}
