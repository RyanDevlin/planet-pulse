from setuptools import setup, find_packages


data_files = [
    (
        'intake/sources/co2_weekly_mlo',
        [
            'intake/sources/co2_weekly_mlo/co2_weekly_mlo_config.yml'
        ]
    )
]

setup(
    name="intake",
    version="1.0",
    packages=find_packages(),
    data_files=data_files
)