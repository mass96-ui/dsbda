#!/usr/bin/env python3
"""
Forest Fire Dataset Analysis using MapReduce (mrjob)
Task: Calculate average burned area (hectares) per month.
Assumes standard UCI Forest Fire CSV format:
X,Y,month,day,FFMC,DMC,DC,ISI,temp,RH,wind,rain,area
"""

from mrjob.job import MRJob

class ForestFireAvgAreaByMonth(MRJob):

    def mapper(self, _, line):
        """
        Mapper Phase:
        - Skips header & empty lines
        - Extracts month and burned area
        - Emits: (month, (area, 1))
        """
        line = line.strip()
        if not line or line.lower().startswith("x,y"):
            return

        parts = line.split(',')
        # Standard UCI dataset has 13 columns (index 0-12)
        if len(parts) < 13:
            return

        try:
            month = parts[2].strip()
            area = float(parts[12].strip())
            
            # Optional: Uncomment next line to ignore non-fire records (area == 0)
            # if area == 0.0: return
            
            yield month, (area, 1)
        except ValueError:
            # Skip rows with malformed numeric data
            return

    def reducer(self, month, values):
        """
        Reducer Phase:
        - Aggregates total area and record count per month
        - Computes and emits average area
        """
        total_area = 0.0
        total_count = 0
        for area, count in values:
            total_area += area
            total_count += count

        avg_area = total_area / total_count if total_count > 0 else 0.0
        yield month, round(avg_area, 4)


if __name__ == '__main__':
    ForestFireAvgAreaByMonth.run()
