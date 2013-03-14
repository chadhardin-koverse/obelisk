/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package cehardin.obelisk.impl;

import cehardin.obelisk.Agent;
import cehardin.obelisk.Agent.Id;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 *
 * @author Chad
 */
public class BasicReactor implements Reactor {

	public void feed(Id from, Id to, Object message) {
		messagePackages.offer(new Message(from, to, message));
	}

	public void add(Id id, Agent agent) {
		agents.putIfAbsent(id, agent);
	}

	public void remove(Id id) {
		agents.remove(id);
	}

	protected static class MessagePackage {

		private final Id from;
		private final Id to;
		private final Object message;

		public MessagePackage(Id from, Id to, Object message) {
			this.from = from;
			this.to = to;
			this.message = message;
		}

		public Id getFrom() {
			return from;
		}

		public Id getTo() {
			return to;
		}

		public Object getMessage() {
			return message;
		}
	}

	private class Procesor implements Runnable {

		public void run() {
			while (true) {
				MessagePackage messagePackage = messagePackages.take();
				Worker worker = new Worker(messagePackage);
				executor.execute(worker);
			}
		}
	}

	private class AgentContext implements Agent.Context {

		private final MessagePackage messagePackage;

		public AgentContext(MessagePackage messagePackage) {
			this.messagePackage = messagePackage;
		}

		

		public Object getMessage() {
			return messagePackage.getMessage();
		}

		public Id getSender() {
			return messagePackage.getFrom();
		}

		public Id getId() {
			return messagePackage.getTo();
		}

		public Id getRootId() {
			throw new UnsupportedOperationException("Not supported yet.");
		}

		public void stop() {
			remove(messagePackage.getTo());
		}

		public void send(Id destination, Object message) {
			feed(messagePackage.getTo(), destination, message);
		}

		public Id create(Class<? extends Agent> agentClass) {
			Id id = null;
			add(id, agentClass.newInstance());
			return id;
		}
	}

	private class Worker implements Runnable {

		private final MessagePackage messagePackage;

		public Worker(MessagePackage messagePackage) {
			this.messagePackage = messagePackage;
		}

		public void run() {
			final Id to = messagePackage.getTo();
			if (agents.containsKey(to)) {
				runningAgents.putIfAbsent(to, false);
				if (runningAgents.replace(to, false, true)) {
					try {
						final Agent agent = agents.get(to);

						if (agent != null) {
							agent.execute(new AgentContext(messagePackage));
						}
					} finally {
						if (!runningAgents.replace(to, true, false)) {
							throw new IllegalStateException();
						}
					}
				} else {
					messagePackages.add(messagePackage);
				}
			}
		}
	}
	private Executor executor;
	private final ConcurrentMap<Id, Agent> agents;
	private final ConcurrentMap<Id, Boolean> runningAgents;
	private final BlockingQueue<MessagePackage> messagePackages;

	public BasicReactor(ConcurrentMap<Id, Agent> agents, ConcurrentMap<Id, Boolean> runningAgents, BlockingQueue<MessagePackage> messagePackages) {
		this.agents = agents;
		this.runningAgents = runningAgents;
		this.messagePackages = messagePackages;
		
		executor.execute(new Procesor());
	}
	
	
}
