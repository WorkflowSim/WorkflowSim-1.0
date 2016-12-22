package org.workflowsim.utils;

import org.workflowsim.planning.PSOPlanningAlgorithm;

import net.sourceforge.jswarm_pso.Particle;

public class MyParticle extends Particle {

	public MyParticle() {
		super(PSOPlanningAlgorithm.DIMENSION);
	}

}