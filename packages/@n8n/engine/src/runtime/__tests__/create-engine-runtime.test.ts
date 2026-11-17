import type { DataSource } from '@n8n/typeorm';
import request from 'supertest';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { AllowAllAdmittance } from '../../admittance';
import { mintIdentityToken, SharedSecretIdentityVerifier } from '../../auth';
import { UnexpectedError } from '../../common';
import { TypeOrmExecutionStore, TypeOrmStepStore, type EngineStores } from '../../database';
import { BatchingLifecycleEventPublisher } from '../../lifecycle-events';
import { InMemoryWorkQueue } from '../../queue';
import { noopExecutionResponseSender } from '../../response-channel';
import { RetentionSweeper } from '../../retention';
import { createEngineRuntime } from '../create-engine-runtime';

vi.mock('../../queue', async (importOriginal) => {
	const actual = await importOriginal<typeof import('../../queue')>();
	return { ...actual, InMemoryWorkQueue: vi.fn(actual.InMemoryWorkQueue) };
});

vi.mock('../../retention', async (importOriginal) => {
	const actual = await importOriginal<typeof import('../../retention')>();
	return { ...actual, RetentionSweeper: vi.fn(actual.RetentionSweeper) };
});

/** Enough of a `DataSource` for the stores: they only hold on to a repository. */
const fakeDataSource = () => ({ getRepository: vi.fn(() => ({})) }) as unknown as DataSource;

const secret = 'a'.repeat(32);
const identityVerifier = new SharedSecretIdentityVerifier(secret);
const token = mintIdentityToken(secret, { cpId: 'cp-1', tenantId: 'tenant-1' });
const responseSender = noopExecutionResponseSender;

const runtime = () =>
	createEngineRuntime({
		dataSource: fakeDataSource(),
		admittance: new AllowAllAdmittance(),
		identityVerifier,
		responseSender,
	});

describe('createEngineRuntime', () => {
	afterEach(() => {
		vi.restoreAllMocks();
	});

	it('mounts the execution API behind authentication', async () => {
		const unauthenticated = await request(runtime().app).post('/api/workflow-executions').send({});
		expect(unauthenticated.status).toBe(401);

		// a rejected body proves the route is mounted without reaching the store
		const response = await request(runtime().app)
			.post('/api/workflow-executions')
			.set('Authorization', `Bearer ${token}`)
			.send({});

		expect(response.status).toBe(400);
	});

	it('does not parse an unauthenticated request body', async () => {
		const response = await request(runtime().app)
			.post('/api/workflow-executions')
			.set('Content-Type', 'application/json')
			.send('{');

		expect(response.status).toBe(401);
		expect(response.body).toEqual({ error: 'unauthenticated' });
	});

	it('serves the healthcheck', async () => {
		const response = await request(runtime().app).get('/healthz');

		expect(response.status).toBe(200);
		expect(response.body).toEqual({ status: 'ok' });
	});

	it('hands the engine stores to the external dependencies', () => {
		let stores: EngineStores | undefined;

		createEngineRuntime({
			dataSource: fakeDataSource(),
			admittance: new AllowAllAdmittance(),
			identityVerifier,
			responseSender,
			externalDependencies: (given) => {
				stores = given;
				return {};
			},
		});

		expect(stores?.executionStore).toBeDefined();
		expect(stores?.stepStore).toBeDefined();
	});

	it('builds the external dependencies exactly once', () => {
		// Three handlers share the result, so building twice would split them.
		const build = vi.fn().mockReturnValue({});

		createEngineRuntime({
			dataSource: fakeDataSource(),
			admittance: new AllowAllAdmittance(),
			identityVerifier,
			responseSender,
			externalDependencies: build,
		});

		expect(build).toHaveBeenCalledOnce();
	});

	it('stops the batching publisher it builds for a host lifecycle event callback', async () => {
		// Only the wiring is this test's business: a host callback gets a batching
		// publisher, and stopping the engine stops it.
		const flushOnStop = vi.spyOn(BatchingLifecycleEventPublisher.prototype, 'stop');
		const engine = createEngineRuntime({
			dataSource: fakeDataSource(),
			admittance: new AllowAllAdmittance(),
			identityVerifier,
			responseSender,
			externalDependencies: () => ({
				lifecycleEventCallback: vi.fn().mockResolvedValue(undefined),
			}),
		});
		engine.start();

		await expect(engine.stop()).resolves.toBeUndefined();
		expect(flushOnStop).toHaveBeenCalledOnce();
	});

	it('runs up to the configured number of steps at once', async () => {
		// The wait sweeper is the one way into the step queue that needs no
		// database: it announces the steps that the store says are due.
		vi.spyOn(TypeOrmStepStore.prototype, 'nextWaitDeadline').mockResolvedValue(null);
		vi.spyOn(TypeOrmStepStore.prototype, 'resumeDueSteps')
			.mockResolvedValueOnce(
				Array.from({ length: 6 }, (_, index) => ({ id: `step-${index}`, executionId: 'exec-1' })),
			)
			.mockResolvedValue([]);
		let inFlight = 0;
		let maxInFlight = 0;
		let openGate = () => {};
		const gate = new Promise<void>((resolve) => {
			openGate = resolve;
		});
		vi.spyOn(TypeOrmStepStore.prototype, 'claimStep').mockImplementation(async () => {
			inFlight += 1;
			maxInFlight = Math.max(maxInFlight, inFlight);
			await gate;
			inFlight -= 1;
			// Nothing to claim, so the handler ends here.
			return null;
		});
		const engine = createEngineRuntime({
			dataSource: fakeDataSource(),
			admittance: new AllowAllAdmittance(),
			identityVerifier,
			responseSender,
			waitSweepIntervalMs: 1,
			concurrency: { step: 3 },
		});
		engine.start();

		try {
			await vi.waitFor(() => expect(inFlight).toBe(3));
			await new Promise((resolve) => setTimeout(resolve, 50));
			expect(maxInFlight).toBe(3);

			openGate();
			await vi.waitFor(() => {
				expect(TypeOrmStepStore.prototype.claimStep).toHaveBeenCalledTimes(6);
				expect(inFlight).toBe(0);
			});
			expect(maxInFlight).toBe(3);
		} finally {
			openGate();
			await engine.stop();
		}
	});

	it('passes each queue its configured concurrency', () => {
		vi.mocked(InMemoryWorkQueue).mockClear();

		createEngineRuntime({
			dataSource: fakeDataSource(),
			admittance: new AllowAllAdmittance(),
			identityVerifier,
			responseSender,
			concurrency: { orchestration: 2, step: 3 },
		});

		expect(InMemoryWorkQueue).toHaveBeenCalledTimes(2);
		expect(InMemoryWorkQueue).toHaveBeenNthCalledWith(1, expect.anything(), { concurrency: 2 });
		expect(InMemoryWorkQueue).toHaveBeenNthCalledWith(2, expect.anything(), { concurrency: 3 });
	});

	it('starts and stops both workers', async () => {
		const engine = runtime();
		engine.start();

		await expect(engine.stop()).resolves.toBeUndefined();
	});

	it('builds no retention sweeper when the host sets no retention policy', () => {
		vi.mocked(RetentionSweeper).mockClear();

		runtime();

		expect(RetentionSweeper).not.toHaveBeenCalled();
	});

	it('sweeps with the retention policy of the host and deletes files with its file deleter', async () => {
		const findPrunable = vi
			.spyOn(TypeOrmExecutionStore.prototype, 'findPrunableExecutions')
			.mockResolvedValueOnce([{ id: 'exec-1', workflowId: 'wf-1', finishedAt: new Date(0) }])
			.mockResolvedValue([]);
		const countCutoff = { id: 'exec-0', finishedAt: new Date(0) };
		const findCutoff = vi
			.spyOn(TypeOrmExecutionStore.prototype, 'findRetentionCountCutoff')
			.mockResolvedValue(countCutoff);
		vi.spyOn(TypeOrmExecutionStore.prototype, 'deleteEndedExecutions').mockResolvedValue(1);
		const deleteExecutionFiles = vi.fn(async () => {});
		const engine = createEngineRuntime({
			dataSource: fakeDataSource(),
			admittance: new AllowAllAdmittance(),
			identityVerifier,
			responseSender,
			externalDependencies: () => ({ deleteExecutionFiles }),
			retention: { maxAgeMs: 1_000, intervalMs: 1, maxCount: 5 },
		});
		engine.start();

		await vi.waitFor(() =>
			expect(deleteExecutionFiles).toHaveBeenCalledWith({
				workflowId: 'wf-1',
				executionId: 'exec-1',
			}),
		);
		expect(findCutoff).toHaveBeenCalledWith(5);
		expect(findPrunable).toHaveBeenCalledWith({ maxAgeMs: 1_000, limit: 100, countCutoff });
		await engine.stop();
	});

	it.each([
		{ maxAgeMs: -1 },
		{ maxAgeMs: 1e300 },
		{ maxAgeMs: 0, intervalMs: 0 },
		{ maxAgeMs: 0, intervalMs: 0.5 },
		{ maxAgeMs: 0, intervalMs: 2 ** 31 },
		{ maxAgeMs: 0, batchSize: 0 },
		{ maxAgeMs: 0, batchSize: 1.5 },
		{ maxAgeMs: 0, maxCount: -1 },
		{ maxAgeMs: 0, maxCount: 1.5 },
	])('refuses the retention policy %o', (retention) => {
		expect(() =>
			createEngineRuntime({
				dataSource: fakeDataSource(),
				admittance: new AllowAllAdmittance(),
				identityVerifier,
				responseSender,
				retention,
			}),
		).toThrow(UnexpectedError);
	});

	it.each([Number.NaN, Number.POSITIVE_INFINITY, 0, -1, 1.5])(
		'refuses a staleness threshold of %s',
		(queuedMs) => {
			expect(() =>
				createEngineRuntime({
					dataSource: fakeDataSource(),
					admittance: new AllowAllAdmittance(),
					identityVerifier,
					responseSender,
					staleness: { queuedMs, runningStepMs: 60_000 },
				}),
			).toThrow('The staleness threshold "queuedMs" must be a positive whole number');
		},
	);

	it('accepts positive whole staleness thresholds', () => {
		expect(() =>
			createEngineRuntime({
				dataSource: fakeDataSource(),
				admittance: new AllowAllAdmittance(),
				identityVerifier,
				responseSender,
				staleness: { queuedMs: 1, runningStepMs: 60_000 },
			}),
		).not.toThrow();
	});
});
