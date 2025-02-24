/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.cassandra.dht.tokenallocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class GeneticOptimizer<T> {
    private static final int DEFAULT_POPULATION_SIZE = 20;
    private static final int DEFAULT_SURVIVORS_PER_GENERATION = 5;
    private static final double DEFAULT_MUTATION_RATE = 0.01;
    private static final double DEFAULT_CROSSOVER_RATE = 0.01;

    private final GeneticEncoder<T> encoder;
    private final Function<T, Double> fitnessFunction;
    private final Random random;
    private final int populationSize;
    private final int survivorsPerGeneration;
    private final double mutationRate;
    private final double crossoverRate;

    private List<T> population = new ArrayList<>();

    public GeneticOptimizer(GeneticEncoder<T> encoder, Function<T, Double> fitnessFunction)
    {
        this(encoder, fitnessFunction, ThreadLocalRandom.current(), DEFAULT_POPULATION_SIZE, DEFAULT_SURVIVORS_PER_GENERATION, DEFAULT_MUTATION_RATE, DEFAULT_CROSSOVER_RATE);
    }

    public GeneticOptimizer(GeneticEncoder<T> encoder, Function<T, Double> fitnessFunction, Random random, int populationSize, int survivorsPerGeneration, double mutationRate, double crossoverRate)
    {
        this.encoder = encoder;
        this.fitnessFunction = fitnessFunction;
        this.random = random;
        this.populationSize = populationSize;
        this.survivorsPerGeneration = survivorsPerGeneration;
        this.mutationRate = mutationRate;
        this.crossoverRate = crossoverRate;
    }

    public void initialize() {
        population.clear();
        while (population.size() < populationSize)
        {
            population.add(encoder.getRandom());
        }
    }

    public void optimize(int generations)
    {
        for (int i = 0; i < generations; i++)
        {
            List<T> survivors = population.stream().sorted((a, b) -> Double.compare(fitnessFunction.apply(b), fitnessFunction.apply(a))).limit(survivorsPerGeneration).collect(Collectors.toList());
            List<T> nextGeneration = new ArrayList<>(survivors);

            while (nextGeneration.size() < populationSize)
            {
                T parent1 = survivors.get(random.nextInt(survivors.size()));
                T parent2 = survivors.get(random.nextInt(survivors.size()));
                nextGeneration.add(breed(parent1, parent2));
            }

            population = nextGeneration;
        }
    }

    public T getBest() {
        return population.get(0);
    }

    private T breed(T parent1, T parent2)
    {
        byte[] dna1 = encoder.encode(parent1);
        byte[] dna2 = encoder.encode(parent2);
        byte[] crossed = crossover(dna1, dna2);
        byte[] mutated = mutate(crossed);
        return encoder.decode(mutated);
    }

    private byte[] mutate(byte[] dna)
    {
        // Copy dna, but with random chance, set each byte to a random value.
        byte[] mutated = new byte[dna.length];
        for (int i = 0; i < dna.length; i++)
        {
            if (random.nextDouble() < mutationRate)
            {
                mutated[i] = (byte) random.nextInt(256);
            }
            else
            {
                mutated[i] = dna[i];
            }
        }
        return mutated;
    }

    private byte[] crossover(byte[] dna1, byte[] dna2) {
        // Select one parent at random, begin copying bytes from it to the result.
        // At each step, with random chance, switch to the other parent.
        byte[] crossed = new byte[dna1.length];
        byte[][] parents = {dna1, dna2};

        int currentParent = random.nextInt(2);
        for (int i = 0; i < dna1.length; i++)
        {
            if (random.nextDouble() < crossoverRate)
            {
                currentParent = 1 - currentParent;
            }
            crossed[i] = parents[currentParent][i];
        }

        return crossed;
    }
}
