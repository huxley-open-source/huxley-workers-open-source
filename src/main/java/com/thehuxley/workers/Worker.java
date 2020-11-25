package com.thehuxley.workers;

import java.io.IOException;

import com.thehuxley.queue.RabbitMQConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.thehuxley.data.Configurator;

public abstract class Worker extends Thread {

	private static Logger logger = LoggerFactory.getLogger(Worker.class);
	protected int id;
	private int sleepTime;
	private RabbitMQConsumer queue;

	public Worker(int id) throws IOException {
		this.id = id;
		queue = createRabbitMQAConsumer();
		sleepTime = Integer.parseInt(Configurator.getProperty(
				Constants.CONF_FILENAME, "worker.sleep_after_exception"));
	}

	@Override
	public void run() {

		if (logger.isDebugEnabled()) {
			logger.debug("WORKER[" + id + "] Waiting for messages");
		}

		while (true) {
			try {
				Delivery delivery = queue.nextDelivery();
				String msg = new String(delivery.getBody());
				if (logger.isDebugEnabled()) {
					logger.debug("id [" + id + "]: Message received: '"
							+ msg + "'");
				}

				// Faz as atualizacoes necessarias no banco de dados
				boolean success = onMessage(msg);

				if (!success) {
					logger.warn("The worker ["
							+ id
							+ "] failed to perform the updates. ACK not sent to queue server.");
				} else {
					queue.taskCompleted(delivery);
				}

			} catch (Exception e) {
				logger.error("Error in worker[" + id
						+ "]. Please, fix the problem. Sleeping for "
						+ sleepTime + "milliseconds", e);
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e1) {
					logger.error(e1.getMessage(), e1);
				}
			}

		}

	}

	protected abstract RabbitMQConsumer createRabbitMQAConsumer()
			throws IOException;

	protected abstract boolean onMessage(String message);

	public static void main(String[] argv) throws java.io.IOException,
			java.lang.InterruptedException {

		int evaluationInstances = Integer.parseInt(Configurator.getProperty(
				Constants.CONF_FILENAME, "worker.evaluation.instances"));
		int questInstances = Integer.parseInt(Configurator.getProperty(
				Constants.CONF_FILENAME, "worker.quest.instances"));
		int id = 1;
		for (int i = 0; i < evaluationInstances; i++) {
			new EvaluationWorker(id).start();
			id++;
		}
		
		for (int i = 0; i < questInstances; i++) {
			new QuestWorker(id).start();
			id++;
		}

	}

}
