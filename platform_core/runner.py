from __future__ import annotations

import asyncio
import importlib
import os

from platform_core.agent import BaseAgent
from platform_core.config import build_agent_config, load_system_config
from platform_core.observability import init_observability


def _import_agent_class(module_path: str) -> type[BaseAgent]:
    module_name, class_name = module_path.split(":")
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


async def main() -> None:
    system_config_path = os.environ["SYSTEM_CONFIG"]
    agent_name = os.environ["AGENT_NAME"]

    system_config = load_system_config(system_config_path)
    agent_config = build_agent_config(system_config, agent_name)

    init_observability(service_name=agent_name, otel_endpoint=agent_config.otel_endpoint)

    agent_cls = _import_agent_class(agent_config.module)
    agent = agent_cls(agent_config)
    await agent.run()


if __name__ == "__main__":
    asyncio.run(main())
