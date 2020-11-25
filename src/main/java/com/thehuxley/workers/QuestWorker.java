package com.thehuxley.workers;

import java.io.IOException;
import java.util.Properties;

import com.thehuxley.data.Configurator;
import com.thehuxley.data.DataManager;
import com.thehuxley.queue.RabbitMQAbstraction;
import com.thehuxley.queue.RabbitMQConsumer;

public class QuestWorker extends Worker {

	public QuestWorker(int id) throws IOException {
		super(id);
	}

	@Override
	public boolean onMessage(String message) {
		long questId = Long.parseLong(message);
		return DataManager.triggerQuestionnaireWasUpdated(questId);
	}

	@Override
	protected RabbitMQConsumer createRabbitMQAConsumer()
			throws IOException {
		Properties prop = Configurator.getProperties(Constants.CONF_FILENAME);
		return new RabbitMQConsumer(
				prop.getProperty("rabbitmq.quest.queueName"),
				prop.getProperty("rabbitmq.quest.hostName"),
				Integer.parseInt(prop
						.getProperty("rabbitmq.quest.serverdown.sleep")));
	}

}
