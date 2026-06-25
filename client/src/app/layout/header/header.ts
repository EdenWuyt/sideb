import { Component, inject } from '@angular/core';
import { MatBadge } from '@angular/material/badge';
import { MatButton } from '@angular/material/button';
import { MatIcon } from '@angular/material/icon';
import { Router, RouterLink, RouterLinkActive } from "@angular/router";
import { Busy } from '../../core/services/busy';
import { MatProgressBar } from '@angular/material/progress-bar';
import { Cart } from '../../core/services/cart';
import { Account } from '../../core/services/account';
import { MatMenu, MatMenuItem, MatMenuTrigger } from '@angular/material/menu';
import { MatDivider } from '@angular/material/list';

@Component({
  selector: 'app-header',
  imports: [
    MatIcon,
    MatButton,
    MatBadge,
    RouterLink,
    RouterLinkActive,
    MatProgressBar,
    MatMenu,
    MatMenuTrigger,
    MatMenuItem,
    MatDivider
],
  templateUrl: './header.html',
  styleUrl: './header.css',
})
export class Header {
  busyService = inject(Busy);
  cartService = inject(Cart);
  accountService = inject(Account);
  private router = inject(Router);

  logout() {
    this.accountService.logout().subscribe({
      next: () => {
        this.accountService.currentUser.set(null);
        this.router.navigateByUrl('/');
      },
      error: (error) => console.error(error)
    });
  }
}
