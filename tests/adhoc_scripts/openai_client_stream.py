import argparse
import asyncio
import random
import time
from statistics import quantiles
from typing import List, Tuple

from openai import AsyncOpenAI
from tqdm import tqdm

BASE_TOKENS = ["a", "robot", "should", "never", "always", "do", "anything"]


async def get_token(client: AsyncOpenAI, prompt: str, model: str) -> List[str]:
    completion = await client.completions.create(
        model=model,
        prompt=prompt,
        echo=False,
        stream=True,
        logprobs=0,
        max_tokens=40,
        temperature=0,
    )
    texts = []
    async for c in completion:
        texts.append(c.choices[0].text)
    return texts


async def run_test(
    prompts: List[Tuple[int, str]],
    num_concurrent_workers: int,
    client: AsyncOpenAI,
    model: str,
    desc: str,
) -> Tuple[List[str], float]:
    prompts = prompts.copy()
    results = []

    with tqdm(total=len(prompts), desc=desc) as pbar:

        async def worker():
            while prompts:
                index, prompt = prompts.pop()
                start = time.perf_counter()
                texts = await get_token(client=client, prompt=prompt, model=model)
                end = time.perf_counter()
                results.append((index, texts, end - start))
                pbar.update(1)

        workers = [worker() for _ in range(num_concurrent_workers)]
        await asyncio.gather(*workers)
    results.sort(key=lambda x: x[0])
    return [(texts, response_time) for _, texts, response_time in results]


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, required=True)
    parser.add_argument("--distributor_url", type=str, required=True)
    parser.add_argument("--direct_url", type=str, required=True)
    parser.add_argument("--num_requests", type=int, default=20)
    parser.add_argument("--num_concurrent_workers", type=int)
    args = parser.parse_args()

    assert args.num_requests > 20

    prompts = []

    for i in range(args.num_requests):
        random.shuffle(BASE_TOKENS)
        prompts.append((i, " ".join(BASE_TOKENS)))

    direct_client = AsyncOpenAI(
        api_key="EMPTY",
        base_url=args.direct_url,
    )
    via_distributor_and_sidecar_client = AsyncOpenAI(
        api_key="EMPTY",
        base_url=args.distributor_url,
    )
    direct_results = await run_test(
        prompts=prompts,
        num_concurrent_workers=args.num_concurrent_workers,
        client=direct_client,
        model=args.model,
        desc="direct",
    )
    distributor_results = await run_test(
        prompts=prompts,
        num_concurrent_workers=args.num_concurrent_workers,
        client=via_distributor_and_sidecar_client,
        model=args.model,
        desc="distributor",
    )

    direct_response_times = [response_time for _, response_time in direct_results]
    direct_q = quantiles(direct_response_times, n=100, method="inclusive")
    print(
        f"direct:\np50: {round(direct_q[49], 3)} "
        f"p90: {round(direct_q[89], 3)} "
        f"p99: {round(direct_q[98], 3)}"
    )
    print("=" * 15)

    distributor_response_times = [
        response_time for _, response_time in distributor_results
    ]
    distributor_q = quantiles(distributor_response_times, n=100, method="inclusive")
    print(
        f"distributor:\np50: {round(distributor_q[49], 3)} "
        f"p90: {round(distributor_q[89], 3)} "
        f"p99: {round(distributor_q[98], 3)}"
    )
    print("=" * 30)

    not_matching = 0
    for (direct_texts, _), (distributor_texts, _) in zip(
        direct_results, distributor_results
    ):
        if direct_texts != distributor_texts:
            not_matching += 1
            print("direct: ", "".join(direct_texts))
            print("=" * 10)
            print("distributor: ", "".join(distributor_texts))
            print("=" * 25)
    print("not matching: ", not_matching, " total: ", len(distributor_results))


asyncio.run(main())
