import { Component, inject, OnInit, signal } from '@angular/core';
import { Shop } from '../../../core/services/shop';
import { ActivatedRoute } from '@angular/router';
import { Product } from '../../../shared/models/product';
import { CurrencyPipe } from '@angular/common';
import { MatIcon } from '@angular/material/icon';
import { MatButton } from '@angular/material/button';
import { MatFormField, MatInput, MatLabel } from '@angular/material/input';
import { MatDivider } from "@angular/material/divider";

@Component({
  selector: 'app-product-details',
  imports: [
    CurrencyPipe,
    MatButton,
    MatIcon,
    MatFormField,
    MatInput,
    MatLabel,
    MatDivider
],
  templateUrl: './product-details.html',
  styleUrl: './product-details.css',
})
export class ProductDetails implements OnInit {
  private shopService = inject(Shop);
  private activatedRoute = inject(ActivatedRoute);
  protected readonly product = signal<Product | null>(null);

  ngOnInit(): void {
    this.loadProduct();
  }

  loadProduct(): void {
    const id = this.activatedRoute.snapshot.paramMap.get('id');
    if (!id) return;
    this.shopService.getProduct(+id).subscribe({
      next: (product) => this.product.set(product),
      error: (error) => console.error(error),
    });
  }
}
