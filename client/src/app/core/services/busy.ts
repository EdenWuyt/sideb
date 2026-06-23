import { Injectable, signal } from '@angular/core';

@Injectable({
  providedIn: 'root',
})
export class Busy {
  loading = signal(false);
  busyRequestCount = signal(0);

  busy() {
    this.busyRequestCount.update(count => count + 1);
    this.loading.set(true);
  }

  idle() {
    this.busyRequestCount.update(count => Math.max(0, count - 1));
    if (this.busyRequestCount() <= 0) {
      this.busyRequestCount.set(0);
      this.loading.set(false);
    }
  }
}
