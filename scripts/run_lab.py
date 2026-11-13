"""Полный цикл в одном процессе, включая завершение серверов."""
import ensemble,experiments
try:
    ensemble.action('start')
    experiments.quorum()
    experiments.failover(0)
    experiments.failover(1)
finally:
    ensemble.action('stop')
