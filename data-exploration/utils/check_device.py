import torch

print(f"Torch version: {torch.version.cuda}")

cuda_available = torch.cuda.is_available()
print(f"CUDA Available: {cuda_available}")
print(f"Number of GPUs available: {torch.cuda.device_count()}")
if cuda_available:
    print(f"GPU Name: {torch.cuda.get_device_name(0)}")

