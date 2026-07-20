"""Exceções usadas pelo simpleq."""


class SimpleQError(Exception):
    """Erro genérico do simpleq."""


class Empty(SimpleQError):
    """Levantada quando não há itens disponíveis na fila."""


class LeaseExpired(SimpleQError):
    """Levantada quando o lease de um job expirou antes do processamento."""
