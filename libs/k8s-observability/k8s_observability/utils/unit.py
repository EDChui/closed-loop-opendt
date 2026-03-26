class UnitUtils:
    @staticmethod
    def bytes_to_mb(num_bytes: float) -> float:
        return float(num_bytes) / 1_000_000.0

    @staticmethod
    def parse_cpu_to_core(cpu_value) -> float:
        """
        Kubernetes CPU quantities are usually cores or millicores:
        - '500m' = 0.5 core
        - '2'    = 2 cores

        This function parses requested CPU in cores using:
        1000 millicores = 1 core
        Ref: https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu
        """
        if cpu_value is None:
            return 0.0

        s = str(cpu_value).strip()
        if s.endswith("m"):
            return float(s[:-1]) / 1000.0

        return float(s)
    
    @staticmethod
    def parse_mem_to_mb(mem_value) -> int:
        """
        Convert Kubernetes memory quantities to MB (decimal-ish output).
        Supports common forms like:
        - Ki, Mi, Gi, Ti
        - K, M, G, T
        - plain integer bytes
        """
        if mem_value is None:
            return 0

        s = str(mem_value).strip()

        binary_units = {
            "Ki": 1 / 1024,
            "Mi": 1,
            "Gi": 1024,
            "Ti": 1024 * 1024,
        }
        decimal_units = {
            "K": 1 / 1000,
            "M": 1,
            "G": 1000,
            "T": 1000 * 1000,
        }

        for unit, factor in binary_units.items():
            if s.endswith(unit):
                return int(float(s[:-len(unit)]) * factor)

        for unit, factor in decimal_units.items():
            if s.endswith(unit):
                return int(float(s[:-len(unit)]) * factor)

        # plain bytes -> MB
        return int(float(s) / (1000 * 1000))
    
    @staticmethod
    def parse_mem_to_bytes(mem_value) -> int:
        """
        Convert Kubernetes memory quantities to bytes.
        Supports common forms like:
        - Ki, Mi, Gi, Ti
        - K, M, G, T
        - plain integer bytes
        """
        if mem_value is None:
            return 0

        s = str(mem_value).strip()

        binary_units = {
            "Ki": 1024,
            "Mi": 1024 * 1024,
            "Gi": 1024 * 1024 * 1024,
            "Ti": 1024 * 1024 * 1024 * 1024,
        }
        decimal_units = {
            "K": 1000,
            "M": 1000 * 1000,
            "G": 1000 * 1000 * 1000,
            "T": 1000 * 1000 * 1000 * 1000,
        }

        for unit, factor in binary_units.items():
            if s.endswith(unit):
                return int(float(s[:-len(unit)]) * factor)

        for unit, factor in decimal_units.items():
            if s.endswith(unit):
                return int(float(s[:-len(unit)]) * factor)

        # plain bytes
        return int(float(s))
